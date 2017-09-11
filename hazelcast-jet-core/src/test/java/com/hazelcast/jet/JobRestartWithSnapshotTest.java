/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.processor.DiagnosticProcessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.jet.TestUtil.throttle;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class JobRestartWithSnapshotTest extends JetTestSupport {

    private static final int LOCAL_PARALLELISM = 4;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance1;
    private JetInstance instance2;
    private JetTestInstanceFactory factory;


    @Before
    public void setup() {
        factory = new JetTestInstanceFactory();

        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);

        JetInstance[] instances = factory.newMembers(config, 2);
        instance1 = instances[0];
        instance2 = instances[1];

    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void when_nodeDown_then_jobRestartsFromSnapshot() throws InterruptedException {
        DAG dag = new DAG();
        DistributedSupplier<Processor> sup = () -> new StreamSource(100);
        Vertex generator = dag.newVertex("generator", throttle(sup, 1))
                              .localParallelism(1);
        Vertex logger = dag.newVertex("logger", DiagnosticProcessors.writeLogger())
                           .localParallelism(1);

        dag.edge(Edge.between(generator, logger));

        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(2000);
        Job job = instance1.newJob(dag, config);

        int round = 0;
        while (true) {
            Thread.sleep(5000);
            round++;
            if (round == 1) {
                instance2.shutdown();
            }
         }

    }

    static class StreamSource extends AbstractProcessor {

        private final int end;
        private Traverser<Integer> traverser;
        private Integer lastEmitted = - 1;

        public StreamSource(int end) {
            this.end = end;
            this.traverser = getTraverser();
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(traverser,  t -> {
               lastEmitted = t;
            });
        }

        @Override
        public boolean saveSnapshot() {
            System.out.println("Save snapshot");
            System.out.println("State:"  + lastEmitted);
            return tryEmitToSnapshot("next", lastEmitted + 1);
        }

        @Override
        public void restoreSnapshot(@Nonnull Inbox inbox) {
            System.out.println("Restoring snapshot..");
            lastEmitted = ((Map.Entry<Object, Integer>) inbox.poll()).getValue();
            traverser = getTraverser();
        }

        private Traverser<Integer> getTraverser() {
            return Traversers.traverseStream(IntStream.range(lastEmitted + 1, end).boxed());
        }

    }


}