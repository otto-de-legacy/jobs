package de.otto.jobstore.service;

import com.sun.jersey.api.client.UniformInterfaceException;
import de.otto.jobstore.common.Parameter;
import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.common.RemoteJobStatus;
import de.otto.jobstore.service.RemoteJobExecutorService;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;
import de.otto.jobstore.service.exception.RemoteJobNotRunningException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import javax.annotation.Resource;
import java.net.URI;
import java.util.Arrays;

import static org.testng.AssertJUnit.*;

@ContextConfiguration(locations = {"classpath:spring/lhotse-jobs-context.xml"})

public class RemoteJobExecutorServiceTest extends AbstractTestNGSpringContextTests {

    private static final String JOB_NAME = "demojob";

    @Resource
    private RemoteJobExecutorService remoteJobExecutorService;

    @Test(enabled = false)
    public void testStartingDemoJob() throws Exception {
        URI uri = remoteJobExecutorService.startJob(new RemoteJob(JOB_NAME, Arrays.asList(new Parameter("sample_file", "/var/log/syslog"))));
        assertNotNull(uri);
        assertTrue("Expected valid job uri", uri.getPath().startsWith("/jobs/demojob/"));

        remoteJobExecutorService.stopJob(uri);
    }

    @Test(expectedExceptions = RemoteJobAlreadyRunningException.class, enabled = false)
    public void testStartingDemoJobWhichIsAlreadyRunning() throws Exception {
        URI uri = null;
        try {
         uri = remoteJobExecutorService.startJob(new RemoteJob(JOB_NAME, Arrays.asList(new Parameter("sample_file", "/var/log/syslog"))));
        } catch (JobException e) {
            fail("No exception expected when trying to start job");
        }
        assertNotNull(uri);
        assertTrue("Expected valid job uri", uri.getPath().startsWith("/jobs/demojob/"));

        try {
            remoteJobExecutorService.startJob(new RemoteJob(JOB_NAME, Arrays.asList(new Parameter("sample_file", "/var/log/syslog"))));
        } finally {
            remoteJobExecutorService.stopJob(uri);
        }
    }

    @Test(expectedExceptions = RemoteJobNotRunningException.class, enabled = false)
    public void testStoppingJobTwice() throws Exception {
        URI uri = remoteJobExecutorService.startJob(new RemoteJob(JOB_NAME, Arrays.asList(new Parameter("sample_file", "/var/log/syslog"))));
        remoteJobExecutorService.stopJob(uri);
        remoteJobExecutorService.stopJob(uri);
    }

    @Test(expectedExceptions = UniformInterfaceException.class, enabled = false)
    public void testStoppingNotExistingJob() throws Exception {
        remoteJobExecutorService.stopJob(URI.create("http://localhost:5000/jobs/" + JOB_NAME + "/12345"));
    }

    @Test(enabled = false)
    public void testGettingStatusOfRunningJob() throws Exception {
        URI uri = remoteJobExecutorService.startJob(new RemoteJob(JOB_NAME, Arrays.asList(new Parameter("sample_file", "/var/log/syslog"))));
        RemoteJobStatus status = remoteJobExecutorService.getStatus(uri);
        remoteJobExecutorService.stopJob(uri);

        assertNotNull(status);
        assertEquals(RemoteJobStatus.Status.RUNNING, status.status);
        //assertNull(status.result);
    }

    @Test(enabled = false)
    public void testGettingStatusOfFinishedJob() throws Exception {
        URI uri = remoteJobExecutorService.startJob(new RemoteJob(JOB_NAME, Arrays.asList(new Parameter("sample_file", "/var/log/syslog"))));
        remoteJobExecutorService.stopJob(uri);
        RemoteJobStatus status = remoteJobExecutorService.getStatus(uri);

        assertNotNull(status);
        assertEquals(RemoteJobStatus.Status.FINISHED, status.status);
        assertNotNull(status.result);
        assertTrue(status.result.ok);
    }



}
