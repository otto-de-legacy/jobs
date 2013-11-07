package de.otto.jobstore.service;

import de.otto.jobstore.TestSetup;
import de.otto.jobstore.common.ActiveChecker;
import de.otto.jobstore.common.RunningState;
import de.otto.jobstore.common.StoredJobDefinition;
import de.otto.jobstore.repository.JobDefinitionRepository;
import de.otto.jobstore.repository.JobInfoRepository;
import de.otto.jobstore.service.exception.JobServiceNotActiveException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class JobServiceNotActiveTest {

    private JobService jobService;
    private JobInfoRepository jobInfoRepository;
    private JobDefinitionRepository jobDefinitionRepository;
    private static final String JOB_NAME_01 = "test";

    private class AlwaysFalseActiveChecker implements ActiveChecker {
        @Override
        public boolean isActive() {
            return false;
        }
    }

    @BeforeMethod
    public void setUp() throws Exception {
        jobInfoRepository = mock(JobInfoRepository.class);
        jobDefinitionRepository = mock(JobDefinitionRepository.class);
        when(jobDefinitionRepository.find(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName())).thenReturn(StoredJobDefinition.JOB_EXEC_SEMAPHORE);
        jobService = new JobService(jobDefinitionRepository, jobInfoRepository, new AlwaysFalseActiveChecker());
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 1, 1));
    }

    @Test(expectedExceptions = JobServiceNotActiveException.class)
    public void doesNotExecuteJobIfNotActive() throws Exception {
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void doesNotExecuteQueuedJobIfNotActive() throws Exception {
        jobService.executeQueuedJobs();

        verify(jobDefinitionRepository, never()).find(anyString());
    }

    @Test
    public void doesNotPollRemoteJobsIfNotActive() throws Exception {
        jobService.pollRemoteJobs();

        verify(jobDefinitionRepository, never()).find(anyString());
    }

    @Test
    public void doesNotRetryFailedJobsIfNotActive() throws Exception {
        jobService.retryFailedJobs();

        verify(jobInfoRepository, never()).findMostRecentFinished(JOB_NAME_01);
    }

    @Test
    public void doesNotCleanupOldJobsIfNotActive() throws Exception {
        jobService.cleanupOldJobs();

        verify(jobInfoRepository, never()).cleanupOldJobs();
    }

    @Test
    public void doesNotCleanupTimedOutJobsIfNotActive() throws Exception {
        jobService.cleanupTimedOutJobs();

        verify(jobInfoRepository, never()).cleanupTimedOutJobs();
    }

}
