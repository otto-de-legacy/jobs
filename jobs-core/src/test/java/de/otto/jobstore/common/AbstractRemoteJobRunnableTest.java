package de.otto.jobstore.common;

import de.otto.jobstore.TestSetup;
import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.service.JobInfoService;
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
    private JobInfoService jobInfoService;

    private Map<String, String> parameters = new HashMap<>();
    private String jobName = "testJob";
    private AbstractRemoteJobDefinition jobDefinition = TestSetup.remoteJobDefinition(jobName, 0, 0);

    @BeforeMethod
    public void setUp() throws Exception {
        remoteJobExecutorService = mock(RemoteJobExecutorService.class);
        jobInfoService = mock(JobInfoService.class);
        parameters.put("key", "value");
    }

    @Test
    public void testRemoteJobSetup() throws Exception {
        URI uri = URI.create("http://www.otto.de");
        JobInfo jobInfo = mock(JobInfo.class);
        when(jobInfo.getParameters()).thenReturn(parameters);
        when(jobInfoService.getById("4811")).thenReturn(jobInfo);
        when(remoteJobExecutorService.startJob(new RemoteJob(jobName, "4811", parameters))).thenReturn(uri);
        JobRunnable runnable = TestSetup.remoteJobRunnable(remoteJobExecutorService, jobInfoService, parameters, jobDefinition);
        MockJobLogger logger = new MockJobLogger();
        JobExecutionContext context = new JobExecutionContext("4811", logger, mock(JobInfoCache.class), JobExecutionPriority.CHECK_PRECONDITIONS, jobDefinition);
        runnable.execute(context);

        assertEquals(uri.toString(), logger.additionalData.get(JobInfoProperty.REMOTE_JOB_URI.val()));
    }

    @Test
    public void testExecutingJobWhichIsAlreadyRunning() throws Exception {
        URI uri = URI.create("http://www.otto.de");
        JobInfo jobInfo = mock(JobInfo.class);
        when(jobInfo.getParameters()).thenReturn(parameters);
        when(jobInfoService.getById("4711")).thenReturn(jobInfo);
        when(remoteJobExecutorService.startJob(new RemoteJob(jobName, "4711", parameters))).
                thenThrow(new RemoteJobAlreadyRunningException("", uri));
        JobRunnable runnable = TestSetup.remoteJobRunnable(remoteJobExecutorService, jobInfoService, parameters, jobDefinition);
        MockJobLogger logger = new MockJobLogger();
        JobExecutionContext context = new JobExecutionContext("4711", logger, mock(JobInfoCache.class), JobExecutionPriority.CHECK_PRECONDITIONS, jobDefinition);
        runnable.execute(context);

        assertEquals(uri.toString(), logger.additionalData.get(JobInfoProperty.REMOTE_JOB_URI.val()));
        assertEquals(uri.toString(), logger.additionalData.get("resumedAlreadyRunningJob"));
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

        @Override
        public String getAdditionalData(String key) {
            return additionalData.get(key);
        }
    }

}
