package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.common.RemoteJobStatus;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;
import de.otto.jobstore.service.exception.RemoteJobNotRunningException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import javax.annotation.Resource;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.*;

@ContextConfiguration(locations = {"classpath:spring/jobs-context.xml"})
public class RemoteJobExecutorServiceWithScriptTransferIntegrationTest extends AbstractTestNGSpringContextTests {

    private static final String JOB_NAME = "demojob1";
    public static final boolean ENABLE_TESTS = false;

    @Resource
    private RemoteJobExecutorWithScriptTransferService remoteJobExecutorService;

    @Test(enabled = ENABLE_TESTS)
    public void testStartingDemoJob() throws Exception {

        URI uri = remoteJobExecutorService.startJob(createRemoteJob());
        assertNotNull(uri);

        remoteJobExecutorService.stopJob(uri);
    }

    @Test(enabled = ENABLE_TESTS, expectedExceptions = RemoteJobAlreadyRunningException.class)
    public void testStartingDemoJobWhichIsAlreadyRunning() throws Exception {
        URI uri = null;
        try {
            uri = remoteJobExecutorService.startJob(createRemoteJob());
        } catch (JobException e) {
            fail("No exception expected when trying to start job");
        }
        assert uri != null;
        assertTrue("Expected valid job uri", uri.getPath().startsWith("/jobs/" + JOB_NAME));

        try {
            remoteJobExecutorService.startJob(createRemoteJob());
        } finally {
            remoteJobExecutorService.stopJob(uri);
        }
    }

    @Test(enabled = ENABLE_TESTS, expectedExceptions = RemoteJobNotRunningException.class)
    public void testStoppingJobTwice() throws Exception {
        URI uri = remoteJobExecutorService.startJob(createRemoteJob());
        remoteJobExecutorService.stopJob(uri);
        remoteJobExecutorService.stopJob(uri);
    }

    @Test(enabled = ENABLE_TESTS, expectedExceptions = RemoteJobNotRunningException.class)
    public void testStoppingNotExistingJob() throws Exception {
        remoteJobExecutorService.stopJob(URI.create(remoteJobExecutorService.getJobExecutorUri() + JOB_NAME + "/12345"));
    }


    class GetRequest implements Runnable {

        private URI uri;

        public GetRequest(URI uri) {
            this.uri = uri;
        }

        @Override
        public void run() {
            RemoteJobStatus status = remoteJobExecutorService.getStatus(uri);
            assertNotNull(status);
            assertEquals(RemoteJobStatus.Status.RUNNING, status.status);
        }
    }

    @Test(enabled = ENABLE_TESTS)
    public void testGettingStatusOfRunningJob() throws Exception {
        final URI uri = remoteJobExecutorService.startJob(createRemoteJob());

        ExecutorService exec = Executors.newFixedThreadPool(8);
        for (int i = 0; i < 100; i++) {
            exec.submit(new GetRequest(uri));
        }
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);

        remoteJobExecutorService.stopJob(uri);
    }

    @Test(enabled = ENABLE_TESTS)
    public void testGettingStatusOfFinishedJob() throws Exception {
        URI uri = remoteJobExecutorService.startJob(createRemoteJob());
        remoteJobExecutorService.stopJob(uri);
        RemoteJobStatus status = remoteJobExecutorService.getStatus(uri);

        assertNotNull(status);
        assertEquals(RemoteJobStatus.Status.FINISHED, status.status);
        assertNotNull(status.result);
        // Job was aborted
        assertFalse(status.result.ok);
        assertEquals(-1, status.result.exitCode);
    }

    private RemoteJob createRemoteJob() {
        Map<String, String> params = new HashMap<String, String>();
        params.put("host", "127.0.0.1");
        return new RemoteJob(JOB_NAME, "2311", params);
    }

}
