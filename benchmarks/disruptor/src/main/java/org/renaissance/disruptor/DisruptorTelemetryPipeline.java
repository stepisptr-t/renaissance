package org.renaissance.disruptor;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.agrona.collections.LongHashSet;
import org.renaissance.Benchmark;
import org.renaissance.BenchmarkContext;
import org.renaissance.BenchmarkResult;
import org.renaissance.License;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.renaissance.Benchmark.*;
import static org.renaissance.BenchmarkResult.Validators.*;

@Name("disruptor-telemetry")
@Group("concurrency")
@Summary("High-throughput telemetry trend analysis pipeline.")
@Licenses(License.APACHE2)
@Parameter(name = "event_count", defaultValue = "1000000", summary = "Total number of full telemetry events to process.")
@Parameter(name = "ring_size", defaultValue = "65536", summary = "Size of the Disruptor RingBuffer (must be power of 2).")
@Parameter(name = "producer_threads", defaultValue = "$cpu.count", summary = "Number of parallel producer threads.")
@Configuration(name = "test", settings = {"event_count = 50000"})
public final class DisruptorTelemetryPipeline implements Benchmark {

    // workload parameters
    private int eventCount;
    private int ringSize;
    private int producerCount;

    // validation containers and counters
    private final Set<Long> expectedFailingDataSources;
    private final Set<Long> detectedFailingDataSources;
    private final AtomicLong totalProcessedEventCount;

    // off-heap sample storage, allocated once before everything
    // used as a data sink at the end and simulates external storage
    private TelemetrySampleStorage sampleStore;

    public DisruptorTelemetryPipeline() {
        expectedFailingDataSources = new LongHashSet();
        for (long id : TelemetryProducer.FAILING_DATA_SOURCE_IDS) {
            expectedFailingDataSources.add(id);
        }
        detectedFailingDataSources = Collections.synchronizedSet(new HashSet<>());
        totalProcessedEventCount = new AtomicLong(0);
    }

    @Override
    public void setUpBeforeAll(BenchmarkContext context) {
        eventCount = context.parameter("event_count").toPositiveInteger();
        ringSize = context.parameter("ring_size").toPositiveInteger();
        producerCount = Math.max(1, context.parameter("producer_threads").toPositiveInteger());

        int maxSamples = (eventCount / DataSampleHandler.SAMPLE_FRQCY) + 1;
        sampleStore = new TelemetrySampleStorage(maxSamples);
    }

    @Override
    public void tearDownAfterAll(BenchmarkContext context) {
        sampleStore = null;
    }

    @Override
    public BenchmarkResult run(BenchmarkContext context) {
        detectedFailingDataSources.clear();
        totalProcessedEventCount.set(0);
        sampleStore.reset();

        final Disruptor<TelemetryEvent> disruptor = new Disruptor<>(
                TelemetryEvent::new,
                ringSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );

        // first the produced partial events are assembled into complete tuples of all data
        disruptor.handleEventsWith(new AssemblerHandler(ringSize))
                // then anomaly detection
                .then(new AnomalyDetectorHandler())
                .then(
                    // then parallel anomalies processing
                    new AnomalyPersistenceHandler(detectedFailingDataSources),
                    // and data sampler of all events to simulate external out of pipeline storage/processing
                    new DataSampleHandler(sampleStore, totalProcessedEventCount)
                );

        RingBuffer<TelemetryEvent> ringBuffer = disruptor.start();

        // 3 producer types, each with an equal share of the total producer threads
        // each producing their share of partial events which are later composed together in the AssemblerHandler
        int producerTypesCount = PartialEventType.values().length;
        int producersPerType = Math.max(1, producerCount / producerTypesCount);
        int totalProducers = producersPerType * producerTypesCount;
        long partialEventsPerProducer = eventCount / producersPerType;

        long expectedAggregatedEvents = partialEventsPerProducer * producersPerType;

        final CountDownLatch latch = new CountDownLatch(totalProducers);

        // old java doesn't support try with resources on this executor ):
        ExecutorService producerExecutor = Executors.newFixedThreadPool(totalProducers);
        try {
            for (int producerId = 0; producerId < producersPerType; producerId++) {
                producerExecutor.submit(new TelemetryProducer(PartialEventType.DATA_SOURCE_ID, producerId, partialEventsPerProducer, latch, ringBuffer));
                producerExecutor.submit(new TelemetryProducer(PartialEventType.TORQUE, producerId, partialEventsPerProducer, latch, ringBuffer));
                producerExecutor.submit(new TelemetryProducer(PartialEventType.TEMPERATURE, producerId, partialEventsPerProducer, latch, ringBuffer));
            }

            latch.await();
            disruptor.shutdown(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            producerExecutor.shutdownNow();
        }

        return compound(
                simple("aggregated events", expectedAggregatedEvents, totalProcessedEventCount.get()),
                simple("sampled events", expectedAggregatedEvents / DataSampleHandler.SAMPLE_FRQCY, sampleStore.sampleCount()),
                hashing("        5930fa5d", detectedFailingDataSources)
        );
    }
}
