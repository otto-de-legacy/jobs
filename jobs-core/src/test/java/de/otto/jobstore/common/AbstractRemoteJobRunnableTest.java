package de.otto.jobstore.common;

import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.service.RemoteJobExecutorService;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

public class AbstractRemoteJobRunnableTest {

    private RemoteJobExecutorService remoteJobExecutorService;
    private Map<String, String> parameters = new HashMap<String, String>();
    private String jobName = "testJob";

    @BeforeMethod
    public void setUp() throws Exception {
        remoteJobExecutorService = mock(RemoteJobExecutorService.class);
        parameters.put("key", "value");
    }

    @Test
    public void testExecutingJob() throws Exception {
        URI uri = URI.create("http://www.otto.de");
        when(remoteJobExecutorService.startJob(new RemoteJob(jobName, "4811", parameters))).thenReturn(uri);
        JobRunnable runnable = new RemoteJobRunnable(remoteJobExecutorService, "4811");
        MockJobLogger logger = new MockJobLogger();
        JobExecutionContext context = new JobExecutionContext(logger, JobExecutionPriority.CHECK_PRECONDITIONS);
        runnable.execute(context);

        assertEquals(uri.toString(), logger.additionalData.get(JobInfoProperty.REMOTE_JOB_URI.val()));
    }

    @Test
    public void testExecutingJobWhichIsAlreadyRunning() throws Exception {
        URI uri = URI.create("http://www.otto.de");
        when(remoteJobExecutorService.startJob(new RemoteJob(jobName, "4711", parameters))).
                thenThrow(new RemoteJobAlreadyRunningException("", uri));
        JobRunnable runnable = new RemoteJobRunnable(remoteJobExecutorService, "4711");
        MockJobLogger logger = new MockJobLogger();
        JobExecutionContext context = new JobExecutionContext(logger, JobExecutionPriority.CHECK_PRECONDITIONS);
        runnable.execute(context);

        assertEquals(uri.toString(), logger.additionalData.get(JobInfoProperty.REMOTE_JOB_URI.val()));
        assertEquals(uri.toString(), logger.additionalData.get("resumedAlreadyRunningJob"));
    }

    private class RemoteJobRunnable extends AbstractRemoteJobRunnable {

        private RemoteJobRunnable(RemoteJobExecutorService remoteJobExecutorService, String id) {
            super(remoteJobExecutorService);
            setId(id);
        }

        @Override
        public Map<String, String> getParameters() {
            return parameters;
        }

        @Override
        public String getName() {
            return jobName;
        }

        @Override
        public long getMaxExecutionTime() {
            return 0;
        }

        @Override
        public long getPollingInterval() {
            return 0;
        }
    }


    private class MockJobLogger implements JobLogger {

        public List<String> logs = new ArrayList<>();
        public Map<String, String> additionalData = new HashMap<>();


        @Override
        public void addLoggingData(String log) {
            logs.add(log);
        }

        @Override
        public List<String> getLoggingData() {
            return logs;
        }

        @Override
        public void insertOrUpdateAdditionalData(String key, String value) {
            additionalData.put(key, value);
        }
    }

}
