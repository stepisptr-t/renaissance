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
import org.renaissance.disruptor.handlers.AnomalyDetectorHandler;
import org.renaissance.disruptor.handlers.AnomalyPersistenceHandler;
import org.renaissance.disruptor.handlers.AssemblerHandler;
import org.renaissance.disruptor.handlers.DataSampleHandler;
import org.renaissance.disruptor.storage.*;
import org.renaissance.disruptor.util.TelemetryEvent;
import org.renaissance.disruptor.util.TelemetryProducer;

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
@Repetitions(30)
@Parameter(name = "events_per_producer", defaultValue = "2000000", summary = "Number of events to produce per producer thread. (At least 100k)")
@Parameter(name = "ring_size", defaultValue = "131072", summary = "Size of the LMAX Disruptor RingBuffer (should be power of 2).")
@Parameter(name = "storage_strategy", defaultValue = "sbebuffer", summary = "Storage strategy for partial events (sbebuffer, caffeine, hashmap).")
@Configuration(name = "test", settings = {"events_per_producer = 50000"})
public final class DisruptorTelemetryPipeline implements Benchmark {
    // workload parameters
    private int eventsPerProducer;
    private int ringSize;
    private int producerCount;
    private String storageStrategy;

    // validation containers and counters
    private final Set<Long> expectedFailingDataSources;
    private final Set<Long> detectedFailingDataSources;
    private final AtomicLong totalProcessedEventCount;

    // off-heap sample storage, allocated once before everything
    // used as a data sink at the end and simulates external storage
    private TelemetrySampleStorage sampleStore;

    // storage of partial events with limited lifetime
    private PartialTelemetryStorage partialTelemetryStorage;
    public static final int PARTIAL_EVENT_LIFETIME_MILLIS = 2500;

    public DisruptorTelemetryPipeline() {
        expectedFailingDataSources = new LongHashSet();
        expectedFailingDataSources.addAll(TelemetryProducer.FAILING_DATA_SOURCE_IDS);
        detectedFailingDataSources = Collections.synchronizedSet(new HashSet<>());
        totalProcessedEventCount = new AtomicLong(0);
    }

    @Override
    public void setUpBeforeAll(BenchmarkContext context) {
        eventsPerProducer = context.parameter("events_per_producer").toPositiveInteger();
        ringSize = context.parameter("ring_size").toPositiveInteger();
        storageStrategy = context.parameter("storage_strategy").value().toLowerCase();

        if (eventsPerProducer < 100_000) {
            throw new IllegalArgumentException("events_per_producer should be at least 100k");
        }

        int producerTypesCount = PartialEventType.values().length;
        int handlerCount = 4; // assembler, anomaly detector, anomaly persistence, data sample handler
        
        producerCount = Math.max(producerTypesCount, Math.min(16, Runtime.getRuntime().availableProcessors() - handlerCount));

        int producersPerType = Math.max(1, producerCount / producerTypesCount);

        int maxSamples = (int) ((((long) eventsPerProducer * producersPerType) / DataSampleHandler.SAMPLE_FRQCY) + 1);
        sampleStore = new TelemetrySampleStorage(maxSamples);

        switch (storageStrategy) {
            case "sbebuffer":
                partialTelemetryStorage = new PartialTelemetrySBEBufferStorage(9_000_000, PARTIAL_EVENT_LIFETIME_MILLIS);
                break;
            case "caffeine":
                partialTelemetryStorage = new PartialTelemetryCacheStorage(PARTIAL_EVENT_LIFETIME_MILLIS);
                break;
            case "hashmap":
                partialTelemetryStorage = new PartialTelemetryHashmapStorage();
                break;
            default:
                throw new IllegalArgumentException("Unknown storage strategy: " + storageStrategy);
        }

        // For the sbe buffer, we need a warm-up and estimate the throughput for correct lifetime management
        if (storageStrategy.equals("sbebuffer")) {
            int warmupEventsPerProducer = 200_000;
            // warmup for pagefaults and jit
            for (int i=0 ; i< 10 ; ++i) {
                executePipeline(warmupEventsPerProducer, partialTelemetryStorage);
                tearDownAfterEach(context);
            }

            long startTime = System.nanoTime();
            executePipeline(warmupEventsPerProducer, partialTelemetryStorage);
            long endTime = System.nanoTime();

            double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
            long totalEvents = (long) warmupEventsPerProducer * producerTypesCount * producersPerType;

            // overshooting by about 25% just to be safe with lifetime management and to avoid premature overwriting of partial events
            // the throughput and latency should be rather stable, but sometimes spikes do happen for reasons outside the jvm and we dont want them to cause failures
            int estimatedThroughput = (int) (1.25 * (totalEvents / durationSeconds));

            partialTelemetryStorage = new PartialTelemetrySBEBufferStorage(estimatedThroughput, PARTIAL_EVENT_LIFETIME_MILLIS);
        }

        tearDownAfterEach(context);
    }

    @Override
    public void tearDownAfterEach(BenchmarkContext context) {
        detectedFailingDataSources.clear();
        totalProcessedEventCount.set(0);
        sampleStore.reset();
        partialTelemetryStorage.clear();
    }

    private void executePipeline(int eventsToProduce, PartialTelemetryStorage storage) {
        final Disruptor<TelemetryEvent> disruptor = new Disruptor<>(
                TelemetryEvent::new,
                ringSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );

        disruptor.handleEventsWith(new AssemblerHandler(storage))
                .then(new AnomalyDetectorHandler())
                .then(
                        new AnomalyPersistenceHandler(detectedFailingDataSources),
                        new DataSampleHandler(sampleStore, totalProcessedEventCount)
                );

        RingBuffer<TelemetryEvent> ringBuffer = disruptor.start();

        int producerTypesCount = PartialEventType.values().length;
        int producersPerType = Math.max(1, producerCount / producerTypesCount);
        int totalProducers = producersPerType * producerTypesCount;
        long partialEventsPerProducer = eventsToProduce;

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
            disruptor.shutdown(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            producerExecutor.shutdownNow();
        }
    }

    @Override
    public BenchmarkResult run(BenchmarkContext context) {
        executePipeline(eventsPerProducer, partialTelemetryStorage);

        int producerTypesCount = PartialEventType.values().length;
        int producersPerType = Math.max(1, producerCount / producerTypesCount);

        long expectedAggregatedEvents = (long) eventsPerProducer * producersPerType;

        return compound(
                simple("aggregated events", expectedAggregatedEvents, totalProcessedEventCount.get()),
                simple("sampled events", expectedAggregatedEvents / DataSampleHandler.SAMPLE_FRQCY, sampleStore.sampleCount()),
                simple("samples are not empty",
                        sampleStore.sampleCount(),
                        sampleStore.stream().filter(event ->
                                event.dataSourceId > 0
                                        && Arrays.stream(event.temperatures).allMatch(temp -> temp > 0d)
                                        && Arrays.stream(event.torques).allMatch(torq -> torq > 0d)
                        ).count()
                ),
                collectionEquals("detected anomaly data sources", expectedFailingDataSources, detectedFailingDataSources)
        );
    }
}
