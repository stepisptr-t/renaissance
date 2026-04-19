package org.renaissance.disruptor;

import com.lmax.disruptor.RingBuffer;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

public final class TelemetryProducer implements Runnable {
    public static final long[] FAILING_DATA_SOURCE_IDS = {10, 20, 30, 40, 50};
    private static final long NORMAL_DATA_SOURCE_ID_BASE = 1000L;
    private static final int FAILING_DATA_SOURCE_PROBABILITY_PCT = 1;

    private static final SensorConfig TORQUE_CONFIG = new SensorConfig(50.0, 450.0, 100.0, 10.0);
    private static final SensorConfig TEMP_CONFIG = new SensorConfig(20.0, 60.0, 40.0, 5.0);

    private final PartialEventType type;
    private final int producerId;
    private final long eventsPerProducer;
    private final CountDownLatch latch;
    private final RingBuffer<TelemetryEvent> ringBuffer;

    public TelemetryProducer(
            PartialEventType type,
            int producerId,
            long eventsPerProducer,
            CountDownLatch latch,
            RingBuffer<TelemetryEvent> ringBuffer
    ) {
        this.type = type;
        this.producerId = producerId;
        this.eventsPerProducer = eventsPerProducer;
        this.latch = latch;
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void run() {
        try {
            final Random randomData = new FastUnsafeRandom(producerId);
            final Random randomTime = new FastUnsafeRandom(producerId);
            final long baseObservationId = producerId * eventsPerProducer;

            for (long i = 0; i < eventsPerProducer; i++) {
                final long observationId = baseObservationId + i;

                randomData.setSeed(observationId);

                // somewhere between 300 and 500 ns busy waiting
                int waitTimeNs = randomTime.nextInt(200) + 300;
                busyWait(waitTimeNs);

                final long sequence = ringBuffer.next();
                try {
                    final TelemetryEvent event = ringBuffer.get(sequence);
                    event.observationId = observationId;
                    event.type = type;
                    event.isReady = false;
                    event.isAnomaly = false;

                    boolean isFailing = randomData.nextInt(100) < FAILING_DATA_SOURCE_PROBABILITY_PCT;
                    double progress = (double) i / eventsPerProducer;

                    switch (type) {
                        case DATA_SOURCE_ID:
                            event.dataSourceId = isFailing
                                    ? FAILING_DATA_SOURCE_IDS[(int)(observationId % FAILING_DATA_SOURCE_IDS.length)]
                                    : NORMAL_DATA_SOURCE_ID_BASE + (observationId % 1000);
                            break;
                        case TORQUE:
                            produceSensorData(TORQUE_CONFIG, event, isFailing, progress, randomData);
                            break;
                        case TEMPERATURE:
                            produceSensorData(TEMP_CONFIG, event, isFailing, progress, randomData);
                            break;
                    }
                } finally {
                    ringBuffer.publish(sequence);
                }
            }
        } finally {
            latch.countDown();
        }
    }


    private static final class SensorConfig {
        final double normalBase;
        final double failingBase;
        final double maxDrift;
        final double noise;

        SensorConfig(double normalBase, double failingBase, double maxDrift, double noise) {
            this.normalBase = normalBase;
            this.failingBase = failingBase;
            this.maxDrift = maxDrift;
            this.noise = noise;
        }
    }

    private void produceSensorData(SensorConfig config, TelemetryEvent event, boolean isFailing, double progress, Random random) {
        double base = isFailing ? config.failingBase : config.normalBase;
        double drift = isFailing ? (progress * config.maxDrift) : 0.0;
        for (int k = 0; k < 6; k++) {
            double noiseScale = (random.nextDouble() * 2.0) - 1.0;
            double value = base + drift + (noiseScale * config.noise);
            if (this.type == PartialEventType.TORQUE) {
                event.torques[k] = value;
            } else {
                event.temperatures[k] = value;
            }
        }
    }

    private void busyWait(long nanoseconds) {
        long end = System.nanoTime() + nanoseconds;
        while (System.nanoTime() < end) {
            Thread.onSpinWait();
        }
    }

}
