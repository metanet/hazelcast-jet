/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.core;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.SnapshotContext;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.impl.util.Util.arrayIndexOf;
import static com.hazelcast.test.PacketFiltersUtil.delayOperationsFrom;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class JobRestartWithSnapshotTest extends JetTestSupport {

    private static final int LOCAL_PARALLELISM = 4;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance1;
    private JetInstance instance2;

    @Before
    public void setup() {
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);

        instance1 = createJetMember(config);
        instance2 = createJetMember(config);
    }

    @Test
    public void when_nodeDown_then_jobRestartsFromSnapshot_singleStage() throws Exception {
        when_nodeDown_then_jobRestartsFromSnapshot(false);
    }

    @Test
    public void when_nodeDown_then_jobRestartsFromSnapshot_twoStage() throws Exception {
        when_nodeDown_then_jobRestartsFromSnapshot(true);
    }

    private void when_nodeDown_then_jobRestartsFromSnapshot(boolean twoStage) throws Exception {
        /* Design of this test:

        It uses a random partitioned generator of source events. The events are
        Map.Entry(partitionId, timestamp). For each partition timestamps from
        0..elementsInPartition are generated.

        We start the test with two nodes and localParallelism(1) for source.
        Source instances generate items at the same rate of 10 per second: this
        causes one instance to be twice as fast as the other in terms of
        timestamp. The source processor saves partition offsets similarly to how
        streamKafka() and streamMap() do.

        After some time we shut down one instance. The job restarts from the
        snapshot and all partitions are restored to single source processor
        instance. Partition offsets are very different, so the source is written
        in a way that it emits from the most-behind partition in order to not
        emit late events from more ahead partitions.

        Local parallelism of InsertWatermarkP is also 1 to avoid the edge case
        when different instances of InsertWatermarkP might initialize with first
        event in different frame and make them start the no-gap emission from
        different WM, which might cause the SlidingWindowP downstream to miss
        some of the first windows.

        The sink writes to an IMap which is an idempotent sink.

        The resulting contents of the sink map are compared to expected value.
         */

        DAG dag = new DAG();

        SlidingWindowPolicy wDef = SlidingWindowPolicy.tumblingWinPolicy(3);
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = counting();

        IMap<List<Long>, Long> result = instance1.getMap("result");
        result.clear();

        SequencesInPartitionsMetaSupplier sup = new SequencesInPartitionsMetaSupplier(3, 200, true);
        Vertex generator = dag.newVertex("generator", throttle(sup, 30))
                              .localParallelism(1);
        Vertex insWm = dag.newVertex("insWm", insertWatermarksP(wmGenParams(
                entry -> ((Entry<Integer, Integer>) entry).getValue(), limitingLag(0), emitByFrame(wDef), -1)))
                          .localParallelism(1);
        Vertex map = dag.newVertex("map",
                mapP((TimestampedEntry e) -> entry(asList(e.getTimestamp(), (long) (int) e.getKey()), e.getValue())));
        Vertex writeMap = dag.newVertex("writeMap", SinkProcessors.writeMapP("result"));

        if (twoStage) {
            Vertex aggregateStage1 = dag.newVertex("aggregateStage1", Processors.accumulateByFrameP(
                    singletonList((DistributedFunction<? super Object, ?>) t -> ((Entry<Integer, Integer>) t).getKey()),
                    singletonList(t1 -> ((Entry<Integer, Integer>) t1).getValue()),
                    TimestampKind.EVENT,
                    wDef,
                    aggrOp.withIdentityFinish()
            ));
            Vertex aggregateStage2 = dag.newVertex("aggregateStage2",
                    combineToSlidingWindowP(wDef, aggrOp, TimestampedEntry::fromWindowResult));

            dag.edge(between(insWm, aggregateStage1)
                    .partitioned(entryKey()))
               .edge(between(aggregateStage1, aggregateStage2)
                       .distributed()
                       .partitioned(entryKey()))
               .edge(between(aggregateStage2, map));
        } else {
            Vertex aggregate = dag.newVertex("aggregate", Processors.aggregateToSlidingWindowP(
                    singletonList((DistributedFunction<Object, Integer>) t -> ((Entry<Integer, Integer>) t).getKey()),
                    singletonList(t1 -> ((Entry<Integer, Integer>) t1).getValue()),
                    TimestampKind.EVENT,
                    wDef,
                    aggrOp,
                    TimestampedEntry::fromWindowResult));

            dag.edge(between(insWm, aggregate)
                    .distributed()
                    .partitioned(entryKey()))
               .edge(between(aggregate, map));
        }

        dag.edge(between(generator, insWm))
           .edge(between(map, writeMap));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(1200);
        Job job = instance1.newJob(dag, config);

        SnapshotRepository snapshotRepository = new SnapshotRepository(instance1);
        int timeout = (int) (MILLISECONDS.toSeconds(config.getSnapshotIntervalMillis()) + 2);

        waitForFirstSnapshot(snapshotRepository, job.getId(), timeout);
        waitForNextSnapshot(snapshotRepository, job.getId(), timeout);
        // wait a little more to emit something, so that it will be overwritten in the sink map
        Thread.sleep(300);

        instance2.shutdown();

        // Now the job should detect member shutdown and restart from snapshot.
        // Let's wait until the next snapshot appears.
        waitForNextSnapshot(snapshotRepository, job.getId(),
                (int) (MILLISECONDS.toSeconds(config.getSnapshotIntervalMillis()) + 10));
        waitForNextSnapshot(snapshotRepository, job.getId(), timeout);

        job.join();

        // compute expected result
        Map<List<Long>, Long> expectedMap = new HashMap<>();
        for (long partition = 0; partition < sup.numPartitions; partition++) {
            long cnt = 0;
            for (long value = 1; value <= sup.elementsInPartition; value++) {
                cnt++;
                if (value % wDef.frameSize() == 0) {
                    expectedMap.put(asList(value, partition), cnt);
                    cnt = 0;
                }
            }
            if (cnt > 0) {
                expectedMap.put(asList(wDef.higherFrameTs(sup.elementsInPartition - 1), partition), cnt);
            }
        }

        // check expected result
        if (!expectedMap.equals(result)) {
            System.out.println("All expected entries: " + expectedMap.entrySet().stream()
                    .map(Object::toString)
                    .collect(joining(", ")));
            System.out.println("All actual entries: " + result.entrySet().stream()
                    .map(Object::toString)
                    .collect(joining(", ")));
            System.out.println("Non-received expected items: " + expectedMap.keySet().stream()
                    .filter(key -> !result.containsKey(key))
                    .map(Object::toString)
                    .collect(joining(", ")));
            System.out.println("Received non-expected items: " + result.entrySet().stream()
                    .filter(entry -> !expectedMap.containsKey(entry.getKey()))
                    .map(Object::toString)
                    .collect(joining(", ")));
            System.out.println("Different keys: ");
            for (Entry<List<Long>, Long> rEntry : result.entrySet()) {
                Long expectedValue = expectedMap.get(rEntry.getKey());
                if (expectedValue != null && !expectedValue.equals(rEntry.getValue())) {
                    System.out.println("key: " + rEntry.getKey() + ", expected value: " + expectedValue
                            + ", actual value: " + rEntry.getValue());
                }
            }
            System.out.println("-- end of different keys");
            assertEquals(expectedMap, new HashMap<>(result));
        }

        assertTrue("Snapshots map not empty after job finished", snapshotRepository.getSnapshotMap(job.getId()).isEmpty());
    }

    @Test
    public void when_snapshotDoneBeforeStarted_then_snapshotSuccessful() {
        /*
        Design of this test

        The DAG is "source -> sink". Source completes immediately on
        non-coordinator (worker) and is infinite on coordinator. Edge between
        source and sink is distributed. This situation will cause that after the
        source completes on member, the sink on worker will only have the remote
        source. This will allow that we can receive the barrier from remote
        coordinator before worker even starts the snapshot. This is the purpose
        of this test. To ensure that this happens, we postpone handling of
        SnapshotOperation on the worker.
        */

        // instance1 is always coordinator
        delayOperationsFrom(
                hz(instance1),
                JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.SNAPSHOT_OPERATION)
        );

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", new NonBalancedSource(getAddress(instance2).toString()));
        Vertex sink = dag.newVertex("sink", writeListP("sink"));
        dag.edge(between(source, sink).distributed());

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(500);
        Job job = instance1.newJob(dag, config);

        assertTrueEventually(() -> assertNotNull(getSnapshotContext(job)));
        SnapshotContext ssContext = getSnapshotContext(job);
        assertTrueEventually(() -> assertTrue("numRemainingTasklets was not negative, the tested scenario did not happen",
                ssContext.getNumRemainingTasklets().get() < 0), 3);
    }

    @Test
    public void when_snapshotStartedBeforeExecution_then_firstSnapshotIsSuccessful() {
        // instance1 is always coordinator
        // delay ExecuteOperation so that snapshot is started before execution is started on the worker member
        delayOperationsFrom(hz(instance1), JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.START_EXECUTION_OP)
        );

        DAG dag = new DAG();
        dag.newVertex("p", FirstSnapshotProcessor::new).localParallelism(1);

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(0);
        Job job = instance1.newJob(dag, config);
        SnapshotRepository repository = new SnapshotRepository(instance1);

        // the first snapshot should succeed
        assertTrueEventually(() -> {
            IMap<Long, SnapshotRecord> records = repository.getSnapshotMap(job.getId());
            SnapshotRecord record = records.get(0L);
            assertNotNull("no record found for snapshot 0", record);
            assertTrue("snapshot was not successful", record.isSuccessful());
        }, 30);
    }

    private SnapshotContext getSnapshotContext(Job job) {
        IMapJet<Long, Long> randomIdsMap = instance1.getMap(JobRepository.RANDOM_IDS_MAP_NAME);
        long executionId = randomIdsMap.entrySet().stream()
                                       .filter(e -> e.getValue().equals(job.getId())
                                               && !e.getValue().equals(e.getKey()))
                                       .mapToLong(Entry::getKey)
                                       .findAny()
                                       .orElseThrow(() -> new AssertionError("ExecutionId not found"));
        ExecutionContext executionContext = null;
        // spin until the executionContext is available on the worker
        while (executionContext == null) {
            executionContext = getJetService(instance2).getJobExecutionService().getExecutionContext(executionId);
        }
        return executionContext.snapshotContext();
    }

    private void waitForFirstSnapshot(SnapshotRepository sr, long jobId, int timeout) {
        assertTrueEventually(() -> assertTrue("No snapshot produced",
                sr.getAllSnapshotRecords(jobId)
                  .stream().anyMatch(SnapshotRecord::isSuccessful)), timeout);
    }

    private void waitForNextSnapshot(SnapshotRepository sr, long jobId, int timeoutSeconds) {
        SnapshotRecord maxRecord = findMaxSuccessfulRecord(sr, jobId);
        assertNotNull("no snapshot found", maxRecord);
        // wait until there is at least one more snapshot
        assertTrueEventually(() -> {
            SnapshotRecord currentMaxRecord = findMaxSuccessfulRecord(sr, jobId);
            assertNotNull("No snapshot record found - job likely finished", currentMaxRecord);
            assertTrue("No more snapshots produced after restart",
                    currentMaxRecord.snapshotId() > maxRecord.snapshotId());
        }, timeoutSeconds);
    }

    private SnapshotRecord findMaxSuccessfulRecord(SnapshotRepository sr, long jobId) {
        return sr.getAllSnapshotRecords(jobId).stream()
                 .filter(SnapshotRecord::isSuccessful)
                 .max(comparing(SnapshotRecord::snapshotId))
                 .orElse(null);
    }

    @Test
    public void when_jobRestartedGracefully_then_noOutputDuplicated() {
        DAG dag = new DAG();
        SequencesInPartitionsMetaSupplier sup = new SequencesInPartitionsMetaSupplier(3, 100, true);
        Vertex generator = dag.newVertex("generator", throttle(sup, 30))
                              .localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeListP("sink"));
        dag.edge(between(generator, sink));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(3600_000); // set long interval so that the first snapshot does not execute
        Job job = instance1.newJob(dag, config);

        // wait for the job to start producing output
        List<Entry<Integer, Integer>> sinkList = instance1.getList("sink");
        assertTrueEventually(() -> assertTrue(sinkList.size() > 10));

        // When
        job.restart();
        job.join();

        // Then
        Set<Entry<Integer, Integer>> expected = IntStream.range(0, sup.elementsInPartition)
                 .boxed()
                 .flatMap(i -> IntStream.range(0, 3).mapToObj(p -> entry(p, i)))
                 .collect(Collectors.toSet());
        assertEquals(expected, new HashSet<>(sinkList));
    }

    @Test
    @Ignore("This is a \"test of a test\" - it checks, that SequencesInPartitionsGeneratorP generates correct output")
    public void test_SequencesInPartitionsGeneratorP() throws Exception {
        SequencesInPartitionsMetaSupplier pms = new SequencesInPartitionsMetaSupplier(3, 2, true);
        pms.init(new TestProcessorMetaSupplierContext().setLocalParallelism(1).setTotalParallelism(2));
        Address a1 = new Address("127.0.0.1", 0);
        Address a2 = new Address("127.0.0.2", 0);
        Function<Address, ProcessorSupplier> supplierFunction = pms.get(asList(a1, a2));
        Iterator<? extends Processor> processors1 = supplierFunction.apply(a1).get(1).iterator();
        Processor p1 = processors1.next();
        assertFalse(processors1.hasNext());
        Iterator<? extends Processor> processors2 = supplierFunction.apply(a2).get(1).iterator();
        Processor p2 = processors2.next();
        assertFalse(processors2.hasNext());

        TestSupport.verifyProcessor(() -> p1).expectOutput(asList(
                entry(0, 0),
                entry(2, 0),
                entry(0, 1),
                entry(2, 1)
        ));

        TestSupport.verifyProcessor(() -> p2).expectOutput(asList(
                entry(1, 0),
                entry(1, 1)
        ));
    }

    @Test
    public void stressTest_restart() throws Exception {
        stressTest(job -> {
            job.restart();
            // Sleep a little because the snapshot that started before restart was requested
            // can complete after this happens.
            LockSupport.parkNanos(SECONDS.toNanos(1));
        });
    }

    @Test
    public void stressTest_stopAndResume() throws Exception {
        stressTest(job -> {
            job.suspend();
            assertTrueEventually(() -> assertEquals(JobStatus.SUSPENDED, job.getStatus()), 5);
            job.resume();
            assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()), 5);
        });
    }

    private void stressTest(Consumer<Job> action) throws Exception {
        SnapshotRepository snapshotRepository = new SnapshotRepository(instance1);

        DAG dag = new DAG();
        dag.newVertex("generator", SnapshotStressSourceP::new)
           .localParallelism(1);

        Job job = instance1.newJob(dag,
                new JobConfig().setSnapshotIntervalMillis(10)
                               .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE));

        waitForFirstSnapshot(snapshotRepository, job.getId(), 5);
        spawn(() -> {
            for (int i = 0; i < 10; i++) {
                action.accept(job);
                waitForNextSnapshot(snapshotRepository, job.getId(), 5);
            }
            return null;
        }).get();

        System.out.println("aaa, cancelling");
        job.cancel();
        try {
            System.out.println("aaa, joining");
            job.join();

        } catch (CancellationException expected) {
            System.out.println("aaa, " + expected + " caught");
        }
        System.out.println("aaa, cancelled");
    }

    private static class SnapshotStressSourceP extends AbstractProcessor {
        private static final int ITEMS_TO_SAVE = 100;

        private Traverser<Map.Entry<BroadcastKey, Integer>> traverser;
        private int numRestored;

        @Override
        public boolean complete() {
            traverser = null;
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            if (traverser == null) {
                traverser = Traversers
                        .traverseStream(IntStream.range(0, ITEMS_TO_SAVE)
                                                 .mapToObj(i -> entry(broadcastKey(i), i)));
            }
            return emitFromTraverserToSnapshot(traverser);
        }

        @Override
        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            numRestored++;
        }

        @Override
        public boolean finishSnapshotRestore() {
            // multiply expected count by 2 because the restored items are broadcast from 2 members
            assertEquals(ITEMS_TO_SAVE * 2, numRestored);
            numRestored = 0;
            return true;
        }
    }

    /**
     * A source, that will generate integer sequences from 0..ELEMENTS_IN_PARTITION,
     * one sequence for each partition.
     * <p>
     * Generated items are {@code entry(partitionId, value)}.
     */
    static class SequencesInPartitionsGeneratorP extends AbstractProcessor {

        private final int[] assignedPtions;
        private final int[] ptionOffsets;
        private final int elementsInPartition;
        private final boolean assertJobRestart;

        private int ptionCursor;
        private MyTraverser traverser;
        private Traverser<Entry<BroadcastKey<Integer>, Integer>> snapshotTraverser;
        private Entry<Integer, Integer> pendingItem;
        private boolean wasRestored;

        SequencesInPartitionsGeneratorP(int[] assignedPtions, int elementsInPartition, boolean assertJobRestart) {
            this.assignedPtions = assignedPtions;
            this.ptionOffsets = new int[assignedPtions.length];
            this.elementsInPartition = elementsInPartition;
            this.assertJobRestart = assertJobRestart;

            this.traverser = new MyTraverser();
        }

        @Override
        protected void init(@Nonnull Context context) {
            getLogger().info("assignedPtions=" + Arrays.toString(assignedPtions));
        }

        @Override
        public boolean complete() {
            boolean res = emitFromTraverserInt(traverser);
            if (res) {
                assertTrue("Reached end of batch without restoring from a snapshot", wasRestored || !assertJobRestart);
            }
            return res;
        }

        @Override
        public boolean saveToSnapshot() {
            // finish emitting any pending item first before starting snapshot
            if (pendingItem != null) {
                if (tryEmit(pendingItem)) {
                    pendingItem = null;
                } else {
                    return false;
                }
            }
            if (snapshotTraverser == null) {
                snapshotTraverser = Traversers.traverseStream(IntStream.range(0, assignedPtions.length).boxed())
                                              // save {partitionId; partitionOffset} tuples
                                              .map(i -> entry(broadcastKey(assignedPtions[i]), ptionOffsets[i]))
                                              .onFirstNull(() -> snapshotTraverser = null);
                getLogger().info("Saving snapshot, offsets=" + Arrays.toString(ptionOffsets) + ", assignedPtions="
                        + Arrays.toString(assignedPtions));
            }
            return emitFromTraverserToSnapshot(snapshotTraverser);
        }

        @Override
        public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            BroadcastKey<Integer> bKey = (BroadcastKey<Integer>) key;
            int partitionIndex = arrayIndexOf(bKey.key(), assignedPtions);
            // restore offset, if assigned to us. Ignore it otherwise
            if (partitionIndex >= 0) {
                ptionOffsets[partitionIndex] = (int) value;
            }
        }

        @Override
        public boolean finishSnapshotRestore() {
            getLogger().info("Restored snapshot, offsets=" + Arrays.toString(ptionOffsets) + ", assignedPtions="
                    + Arrays.toString(assignedPtions));
            // we'll start at the most-behind partition
            advanceCursor();
            wasRestored = true;
            return true;
        }

        // this method is required to keep track of pending item
        private boolean emitFromTraverserInt(MyTraverser traverser) {
            Entry<Integer, Integer> item;
            if (pendingItem != null) {
                item = pendingItem;
                pendingItem = null;
            } else {
                item = traverser.next();
            }
            for (; item != null; item = traverser.next()) {
                if (!tryEmit(item)) {
                    pendingItem = item;
                    return false;
                }
            }
            return true;
        }

        private void advanceCursor() {
            ptionCursor = 0;
            int min = ptionOffsets[0];
            for (int i = 1; i < ptionOffsets.length; i++) {
                if (ptionOffsets[i] < min) {
                    min = ptionOffsets[i];
                    ptionCursor = i;
                }
            }
        }

        private class MyTraverser implements Traverser<Entry<Integer, Integer>> {
            @Override
            public Entry<Integer, Integer> next() {
                try {
                    return ptionOffsets[ptionCursor] < elementsInPartition
                            ? entry(assignedPtions[ptionCursor], ptionOffsets[ptionCursor]) : null;
                } finally {
                    ptionOffsets[ptionCursor]++;
                    advanceCursor();
                }
            }
        }
    }

    static class SequencesInPartitionsMetaSupplier implements ProcessorMetaSupplier {

        private final int numPartitions;
        private final int elementsInPartition;
        private final boolean assertJobRestart;

        private int totalParallelism;
        private int localParallelism;

        SequencesInPartitionsMetaSupplier(int numPartitions, int elementsInPartition, boolean assertJobRestart) {
            this.numPartitions = numPartitions;
            this.elementsInPartition = elementsInPartition;
            this.assertJobRestart = assertJobRestart;
        }

        @Override
        public void init(@Nonnull Context context) {
            totalParallelism = context.totalParallelism();
            localParallelism = context.localParallelism();
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> {
                int startIndex = addresses.indexOf(address) * localParallelism;
                return count -> IntStream.range(0, count)
                                         .mapToObj(index -> new SequencesInPartitionsGeneratorP(
                                                 assignedPtions(startIndex + index, totalParallelism, numPartitions),
                                                 elementsInPartition, assertJobRestart))
                                         .collect(toList());
            };
        }

        private int[] assignedPtions(int processorIndex, int totalProcessors, int numPartitions) {
            return IntStream.range(0, numPartitions)
                            .filter(i -> i % totalProcessors == processorIndex)
                            .toArray();
        }
    }

    /**
     * Supplier of processors that emit nothing and complete immediately
     * on designated member and never on others.
     */
    private static final class NonBalancedSource implements ProcessorMetaSupplier {
        private final String finishingMemberAddress;

        private NonBalancedSource(String finishingMemberAddress) {
            this.finishingMemberAddress = finishingMemberAddress;
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> {
                String sAddress = address.toString();
                return ProcessorSupplier.of(() -> sAddress.equals(finishingMemberAddress)
                        ? noopP().get() : new StreamingNoopSourceP());
            };
        }
    }

    /**
     * A source processor that emits nothing and never completes
     */
    private static final class StreamingNoopSourceP implements Processor {
        @Override
        public boolean complete() {
            return false;
        }
    }

    /**
     * A source processor which never completes and
     * only allows the first snapshot to finish
     */
    private static final class FirstSnapshotProcessor implements Processor {
        private boolean firstSnapshotDone;

        @Override
        public boolean complete() {
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            try {
                return !firstSnapshotDone;
            } finally {
                firstSnapshotDone = true;
            }
        }
    }
}
