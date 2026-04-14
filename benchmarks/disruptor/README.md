# LMAX Disruptor data processing pipeline in Renaissance

### Data model and workload

#### TelemetryEvent
At the beginning of the pipeline, the Event is only a a partial event, containing only one of the data fields.
Upon receiving all fields for an instance of ObservationID inside the `AssemblerHandler`, the final full TelemetryEvent is composed and used in the rest of the pipeline.
 - `observationId: long` - identifier of a logical moment in time of the given observation - is shared between partial events and used as an assembling key
 - `dataSourceID: long` - works as an abstraction over the identifier of the source device, the currently running job and the line number at which the controller was executing the job at the time of the observation.
 - `torques: double[6]` - represents the observed torque for each axis of the robot
 - `temperatures: double[6]` - represents the observed temperature for each axis of the robot


### Data pipeline

The easiest explanation is the code of the nice Disruptor API
```java
// first the produced partial events are assembled into complete tuples of all data
disruptor.handleEventsWith(new AssemblerHandler(ringSize))
        // then anomaly detection
        .then(new AnomalyDetectorHandler(windowSize))
        .then(
            // then parallel anomalies processing
            new AnomalyPersistenceHandler(detectedFailingDataSources),
            // and data sampler of all events to simulate external out of pipeline storage/processing
            new DataSampleHandler(sampleStore, totalProcessedEventCount)
            );
```
Each of these pipeline stages (or Handlers as Disruptor calls them) are running in its own separate thread created by the `DaemonThreadFactory` JDK thread factory, which is specified in the Disruptor constructor.

 - `TelemetryProducer` serves as a producer of data in a its own thread. Each producer produces `TelemetryEvent`s containing either one of `DataSourceID`, `torques` or `temperatures`. 
    - The `producer_threads` parameter is used to split the producers equally for each of the types of partial events. It also influences the total number of producer threads. The `event_count` parameter value is split between each of the producer of the given partial event type.
        ```java
        int producerTypesCount = PartialEventType.values().length;
        int producersPerType = Math.max(1, producerCount / producerTypesCount);
        int totalProducers = producersPerType * producerTypesCount;
        long partialEventsPerProducer = eventCount / producersPerType;
        ```
    - Each data point is identified with a specific ObservationID. For simplification, it is guaranteed that each observation ID contains all DataSourceID, torques and temperatures.
 - `AssemblerHandler` holds a stateful map of incoming partial events and publishes the full event to the next handler only once all three pieces for the given observation id arrive.
    - for the sake of simplification, we are ignoring the possible memory leaks in a production environment where either of the partial events is not actually recevied. Could be solved by using a LRU-like embedded linked list of oldest arrived events and have an O(1) stale events removal, but I did not want to complicate it too much. 
 - `AnomalyDetectionHandler` calculates a RMS for each DataSourceID observed and if the value, which was just observed crosses a certain percentage threshold from the baseline, it tags the event as anomaly.
 - `AnomalyPersistenceHandler` reads all events in the pipeline, which contain the flag that they are an anomaly and stores them.
    - by design of the disruptor model, it has to read all the events in the pipeline, but only use the ones it's interested in (with the flag).
    - This is partially used for validation that the expected job_ids get detected as anomalies.
 - `DataSampleHandler` is a sort of data sink, which just samples every 100 events  received and stores them into a buffer, which is preallocated before the benchmark starts so it doesnt influence the GC during the iterations. It is simulating the storage of data externally out of the pipeline, like to a database etc.

### Design choices

The Disruptor programming model was chosen due to its relative uniqueness in focus on lock-free pipeline processsing with a very impressive throughput for pipelines with multiple producers and consumers. The current concurrent workloads in the Renaissance suite focused on the Actor model (akka and reactor) or JDK task based concurrency. A workload which works in "mechanical sympathy" (as the authors of Disruptor like to say) was missing.

For the specific strategies of the Disruptor model I chose `BusySpinWaitStrategy`, because this maximizes throughput at the cost of some busy spinning upon contention. It is generally appropriate to use for scenarious where, predictable latency is prefered. This specific pipeline is very CPU bound and has a continuous data-flow, therefore the threads dont busy spin for long.

All random number generator use the same seed for every run, therefore each iteration should result in roughly equal workload.

#### Licensing

The LMAX Disruptor library and agrona (for low level containers and utilities), whose libraries I used in the workload are licensed under Apache 2.0 license and the workload is also licensed under the same license 

#### Workload scaling

- `event_count` - Total number of full telemetry events to process. 
    - This is actually the number of partial events per producer type, because we are producing 3 partial events in the producers and then assembling them into the full TelemetryEvent instance in the AssemblerHandler. 
    - Bigger value results in longer runtime.
- `ring_size` - Size of the Disruptor ring buffer. Should be a power of 2, for optimal performance.
    - Making it smaller results in more backpressure for the producers and forces a slow down. 
    - Making it bigger allows the producers to "run away" from the consumers and the consumers can consume the buffer in batches (which is one of the smart features of the Disruptor model).
- `producer_threads` - By default scales to the total number of available CPU threads using the "$cpu.count" shorthand from Reinassaince.
    - Also used to divide the total number of events equally between the producer threads for each partial event.

#### Validation

The validation is rather simple, it validates that all produced events go through the whole pipeline. It also validates that the anomalous data sources are marked as such and that the number of sampled full events at the end is exactly as expected.

#### Preliminary results from local benchmark on OpenJDK

I ran the benchmark on my Thinkpad T14 Gen 2 laptop with AMD Ryzen 5 PRO 5650U and 16GB RAM running NixOS 25.11 with kernel version 6.12.70 and openjdk 21.0.10. 

I used the `jmx-timers` plugin which adds the JIT compilation times information to the results.

The result is stored in the [included file](results.json).

From the results, we can see that the JIT compiler works hardest in the first iteration (415ms of the 1376ms total runtime) and then is still significant for the 7 subsequent iterations. The total runtime then stabilises at around 1050ms.

We can see that the duration of the benchmark stays relatively stable after the first iteration, as the most JIT optimizations were achieved in the first iterations. This stability across iterations is thanks to the deterministic random number generation. The duration of some later iterations 16, 17, 19 dip to about 860ms, without any prior JIT activity. I presume that this can be attributed to external factors present in the system, but I have no evidence to back this up, as some similar dips occur in several different of the later iterations when I ran the benchmark again.

The modest speedup suggests that the workload performance is not dominated by JIT optimized code paths, but rather thanks to the architecture of the Disruptor model which is designed with mechanical sympathy in mind. The spinning overhead upon producer contention will stay there regardless of JIT optimization.
