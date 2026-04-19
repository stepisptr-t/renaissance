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
import java.util.concurrent.locks.LockSupport;

import static org.renaissance.Benchmark.*;
import static org.renaissance.BenchmarkResult.Validators.*;

@Name("disruptor-telemetry")
@Group("concurrency")
@Summary("High-throughput telemetry trend analysis pipeline.")
@Licenses(License.APACHE2)
@Repetitions(10)
@Parameter(name = "producer_threads", defaultValue = "6", summary = "Number of concurrent producer threads (multiple of three is preferable).")
@Parameter(name = "events_per_producer", defaultValue = "2000000", summary = "Number of events to produce per producer thread.")
@Parameter(name = "ring_size", defaultValue = "131072", summary = "Size of the LMAX Disruptor RingBuffer (must be power of 2).")
@Parameter(name = "expected_throughput", defaultValue = "9000000", summary = "Expected throughput of events per second.")
@Parameter(name = "expected_lifetime", defaultValue = "2", summary = "Expected lifetime of an incomplete observation in seconds.")
@Configuration(name = "test", settings = {"events_per_producer = 50000"})
public final class DisruptorTelemetryPipeline implements Benchmark {

    // workload parameters
    private int eventsPerProducer;
    private int ringSize;
    private int producerCount;

    // validation containers and counters
    private final Set<Long> expectedFailingDataSources;
    private final Set<Long> detectedFailingDataSources;
    private final AtomicLong totalProcessedEventCount;

    // off-heap sample storage, allocated once before everything
    // used as a data sink at the end and simulates external storage
    private TelemetrySampleStorage sampleStore;

    // preallocated map for storage of partial events
    private PartialTelemetryMap partialsMap;

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
        eventsPerProducer = context.parameter("events_per_producer").toPositiveInteger();
        ringSize = context.parameter("ring_size").toPositiveInteger();
        producerCount = Math.max(1, context.parameter("producer_threads").toPositiveInteger());
        int expectedThroughput = context.parameter("expected_throughput").toPositiveInteger();
        int expectedLifetime = context.parameter("expected_lifetime").toPositiveInteger();

        int maxSamples = (((eventsPerProducer * producerCount) / PartialEventType.values().length) / DataSampleHandler.SAMPLE_FRQCY) + 1;
        sampleStore = new TelemetrySampleStorage(maxSamples);
        partialsMap = new PartialTelemetryMap(expectedThroughput, expectedLifetime);
    }

    @Override
    public void tearDownAfterAll(BenchmarkContext context) {
        sampleStore = null;
        partialsMap = null;
    }

    @Override
    public void tearDownAfterEach(BenchmarkContext context) {
        detectedFailingDataSources.clear();
        totalProcessedEventCount.set(0);
        sampleStore.reset();
        partialsMap.clear();
    }

    @Override
    public BenchmarkResult run(BenchmarkContext context) {
        final Disruptor<TelemetryEvent> disruptor = new Disruptor<>(
                TelemetryEvent::new,
                ringSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );

        disruptor.handleEventsWith(new AssemblerHandler(partialsMap))
                .then(new AnomalyDetectorHandler())
                .then(
                    new AnomalyPersistenceHandler(detectedFailingDataSources),
                    new DataSampleHandler(sampleStore, totalProcessedEventCount)
                );

        RingBuffer<TelemetryEvent> ringBuffer = disruptor.start();

        int producerTypesCount = PartialEventType.values().length;
        int producersPerType = Math.max(1, producerCount / producerTypesCount);
        int totalProducers = producersPerType * producerTypesCount;
        long partialEventsPerProducer = eventsPerProducer;

        long expectedAggregatedEvents = (long) eventsPerProducer * producersPerType;

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

        return compound(
                simple("aggregated events", expectedAggregatedEvents, totalProcessedEventCount.get()),
                simple("sampled events", expectedAggregatedEvents / DataSampleHandler.SAMPLE_FRQCY, sampleStore.sampleCount()),
                simple("samples are not empty",
                        sampleStore.sampleCount(),
                        sampleStore.stream().filter( event ->
                                event.dataSourceId > 0
                                && Arrays.stream(event.temperatures).allMatch(temp -> temp > 0d)
                                && Arrays.stream(event.torques).allMatch(torq -> torq > 0d)
                        ).count()
                ),
                collectionEquals("detected anomaly data sources", expectedFailingDataSources, detectedFailingDataSources)
        );
    }
}
