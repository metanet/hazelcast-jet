package com.hazelcast.jet.core;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckForeverSourceP;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.JobRestartStrategy;
import com.hazelcast.jet.impl.MasterContext;
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.impl.JobRestartStrategy.AFTER_SUCCESSFUL_SNAPSHOT;
import static com.hazelcast.jet.impl.JobRestartStrategy.IMMEDIATELY;
import static com.hazelcast.jet.impl.JobRestartStrategy.WITH_SHORT_ROLLBACK;
import static com.hazelcast.jet.impl.util.JetGroupProperty.JOB_RESTART_STRATEGY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


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
    public void testJobRestartImmediately() {
        testJobRestart(IMMEDIATELY);
    }

    @Test
    public void testJobRestartAfterSuccessfulSnapshot() {
        testJobRestart(AFTER_SUCCESSFUL_SNAPSHOT);
    }

    private void testJobRestart(JobRestartStrategy jobRestartStrategy) {
        // Given that the submitted job is running
        factory.newMembers(newJetConfig(jobRestartStrategy), NODE_COUNT);
        JetInstance client = factory.newClient();
        jobConfig.setSnapshotIntervalMillis(5_000);
        Job job = client.newJob(dag, jobConfig);

        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT, MockPS.initCount.get());
        });

        // When the job is restarted after a new member joins to the cluster
        factory.newMember();
        job.restart();

        // Then, the job restarts
        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT * 2 + 1, MockPS.initCount.get());
        });
    }

    @Test
    public void testJobRestartWithShortRollbackToLastSnapshot() {
        // Given that the submitted job is running
        JetInstance[] instances = factory.newMembers(newJetConfig(WITH_SHORT_ROLLBACK), NODE_COUNT);
        JetInstance client = factory.newClient();
        jobConfig.setSnapshotIntervalMillis(5_000_000);
        Job job = client.newJob(dag, jobConfig);

        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT, MockPS.initCount.get());
        });

        JetService jetService = getJetService(instances[0]);

        spawn(() -> {
            JobCoordinationService coordinationService = jetService.getJobCoordinationService();
            MasterContext masterContext = coordinationService.getMasterContext(job.getId());
            masterContext.beginSnapshot(masterContext.getExecutionId());
        });

        SnapshotRepository snapshotRepository = jetService.getSnapshotRepository();
        assertTrueEventually(() -> assertNotNull(snapshotRepository.latestCompleteSnapshot(job.getId())));

        // When the job is restarted after a new member joins to the cluster
        factory.newMember();
        job.restart();

        // Then, the job restarts
        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT * 2 + 1, MockPS.initCount.get());
        });
    }

    @Test
    public void testJobRestartWithShortRollbackToNoSnapshot() {
        // Given that the submitted job is running
        factory.newMembers(newJetConfig(WITH_SHORT_ROLLBACK), NODE_COUNT);
        JetInstance client = factory.newClient();
        jobConfig.setSnapshotIntervalMillis(5_000_000);
        Job job = client.newJob(dag, jobConfig);

        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT, MockPS.initCount.get());
        });

        sleepAtLeastSeconds(1);

        // When the job is restarted after a new member joins to the cluster
        factory.newMember();
        job.restart();

        // Then, the job restarts
        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT * 2 + 1, MockPS.initCount.get());
        });
    }

    private JetConfig newJetConfig(JobRestartStrategy strategy) {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().setProperty(JOB_RESTART_STRATEGY.getName(), strategy.name());
        return jetConfig;
    }

}
