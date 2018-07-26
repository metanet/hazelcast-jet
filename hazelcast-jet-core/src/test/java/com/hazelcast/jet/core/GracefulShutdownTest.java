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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class GracefulShutdownTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;

    private JetInstance[] instances;
    private JetInstance client;

    @Before
    public void setup() {
        TestProcessors.reset(0);
        instances = createJetMembers(new JetConfig(), NODE_COUNT);
        client = createJetClient();
        EmitIntegersP.savedCounters.clear();
    }

    @Test
    public void when_snapshottedJob_coordinatorShutDown_then_gracefully() {
        when_shutDown_then_gracefully(true, true);
    }

    @Test
    public void when_snapshottedJob_nonCoordinatorShutDown_then_gracefully() {
        when_shutDown_then_gracefully(false, true);
    }

    @Test
    public void when_nonSnapshottedJob_coordinatorShutDown_then_() {
        when_shutDown_then_gracefully(true, false);
    }

    @Test
    public void when_nonSnapshottedJob_nonCoordinatorShutDown_then_() {
        when_shutDown_then_gracefully(false, false);
    }

    private void when_shutDown_then_gracefully(boolean shutdownCoordinator, boolean snapshotted) {
        DAG dag = new DAG();
        final int numItems = 50_000;
        Vertex source = dag.newVertex("source", throttle(() -> new EmitIntegersP(numItems), 10_000)).localParallelism(1);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("sink"));
        dag.edge(between(source, sink));

        Job job = client.newJob(dag, new JobConfig()
                .setProcessingGuarantee(snapshotted ? EXACTLY_ONCE : NONE)
                .setSnapshotIntervalMillis(HOURS.toMillis(1)));
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()));
        logger.info("sleeping 1 sec");
        sleepSeconds(1);

        int shutDownInstance = shutdownCoordinator ? 0 : 1;
        int liveInstance = shutdownCoordinator ? 1 : 0;

        // When
        logger.info("Shutting down instance...");
        instances[shutDownInstance].shutdown();
        logger.info("Joining job...");
        job.join();
        logger.info("Joined");

        // Then
        // If the shutdown was graceful, output items must not be duplicated
        Map<Integer, Integer> expected;
        Map<Integer, Integer> actual = new ArrayList<>(instances[liveInstance].<Integer>getList("sink")).stream()
                .collect(Collectors.toMap(Function.identity(), item -> 1, Integer::sum));
        if (snapshotted) {
            logger.info("savedCounters=" + EmitIntegersP.savedCounters);
            assertEquals(EmitIntegersP.savedCounters.toString(), 2, EmitIntegersP.savedCounters.size());
            int minCounter = EmitIntegersP.savedCounters.values().stream().mapToInt(Integer::intValue).min().getAsInt();
            expected = IntStream.range(0, numItems).boxed()
                    .collect(Collectors.toMap(Function.identity(), item -> item < minCounter ? 2 : 1));
            assertEquals(expected, actual);
        } else {
            // Items 0, 1, ... up to the point when the member was shut down should be 3 times
            // in the output: twice from before shutdown and one from after it, because it will start
            // from the beginning when the job is not snapshotted.
            assertEquals(3, actual.get(0).intValue());
            assertEquals(3, actual.get(1).intValue());
            assertEquals(1, actual.get(numItems - 1).intValue());
        }
    }

    @Test
    public void when_liteMemberShutDown_then_jobKeepsRunning() throws Exception {
        JetConfig liteMemberConfig = new JetConfig();
        liteMemberConfig.getHazelcastConfig().setLiteMember(true);
        JetInstance liteMember = createJetMember(liteMemberConfig);
        DAG dag = new DAG();
        dag.newVertex("v", (DistributedSupplier<Processor>) StuckProcessor::new);
        Job job = instances[0].newJob(dag);
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()), 10);
        Future future = spawn(liteMember::shutdown);
        assertTrueAllTheTime(() -> assertEquals(RUNNING, job.getStatus()), 5);
        future.get();
    }

    @Test
    public void when_nonParticipatingMemberShutDown_then_jobKeepsRunning() throws Exception {
        DAG dag = new DAG();
        dag.newVertex("v", (DistributedSupplier<Processor>) StuckProcessor::new);
        Job job = instances[0].newJob(dag);
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()), 10);
        Future future = spawn(() -> {
            JetInstance nonParticipatingMember = createJetMember();
            sleepSeconds(1);
            nonParticipatingMember.shutdown();
        });
        assertTrueAllTheTime(() -> assertEquals(RUNNING, job.getStatus()), 5);
        future.get();
    }



    private static final class EmitIntegersP extends AbstractProcessor {
        static final ConcurrentMap<Integer, Integer> savedCounters = new ConcurrentHashMap<>();

        private int counter;
        private int globalIndex;
        private int numItems;
        private boolean emitSucceeded;

        EmitIntegersP(int numItems) {
            this.numItems = numItems;
        }

        @Override
        protected void init(@Nonnull Context context) {
            globalIndex = context.globalProcessorIndex();
        }

        @Override
        public boolean complete() {
            emitSucceeded = tryEmit(counter);
            if (emitSucceeded) {
                counter++;
            }
            return counter == numItems;
        }

        @Override
        public boolean saveToSnapshot() {
            if (!emitSucceeded) {
                complete();
                if (!emitSucceeded) {
                    return false;
                }
            }
            savedCounters.put(globalIndex, counter);
            return tryEmitToSnapshot(broadcastKey(globalIndex), counter);
        }

        @Override
        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            counter = Math.max(counter, (Integer) value);
        }
    }
}
