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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckForeverSourceP;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static org.junit.Assert.assertEquals;


@RunWith(HazelcastSerialClassRunner.class)
public class JobScaleUpTest extends JetTestSupport {

    private static final int NODE_COUNT = 3;

    private static final int LOCAL_PARALLELISM = 1;

    private JetTestInstanceFactory factory;

    private DAG dag;

    private JobConfig jobConfig;

    @Before
    public void setup() {
        MockPS.completeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.completeErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(NODE_COUNT * LOCAL_PARALLELISM);

        factory = new JetTestInstanceFactory();
        dag = new DAG().vertex(new Vertex("test", new MockPS(StuckForeverSourceP::new, NODE_COUNT)));
        jobConfig = new JobConfig().setProcessingGuarantee(AT_LEAST_ONCE);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void when_jobIsRunning_then_itRestarts() {
        // Given that the submitted job is running
        factory.newMembers(new JetConfig(), NODE_COUNT);
        JetInstance client = factory.newClient();
        jobConfig.setSnapshotIntervalMillis(5_000);
        Job job = client.newJob(dag, jobConfig);

        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT, MockPS.initCount.get());
        });

        // When the job is restarted after new members join to the cluster
        int newMemberCount = 2;
        for (int i = 0; i < newMemberCount; i++) {
            factory.newMember();
        }

        job.restart();

        // Then, the job restarts
        int initCount = NODE_COUNT * 2 + newMemberCount;
        assertTrueEventually(() -> {
            assertEquals(initCount, MockPS.initCount.get());
        });
    }

}
