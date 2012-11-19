package de.otto.jobstore.service.impl;

import de.otto.jobstore.common.*;
import de.otto.jobstore.repository.api.JobInfoRepository;
import de.otto.jobstore.service.api.JobService;
import de.otto.jobstore.service.exception.JobNotRegisteredException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.*;

public class JobServiceTest {

    private JobService jobService;
    private JobInfoRepository jobInfoRepository;

    @BeforeMethod
    public void setUp() throws Exception {
        jobInfoRepository = mock(JobInfoRepository.class);
        jobService = new JobServiceImpl(jobInfoRepository, true);
    }

    @Test
    public void testRegisteringJob() throws Exception {
        assertTrue(jobService.registerJob("jobName", createJobInfoCallable()));
        assertFalse(jobService.registerJob("jobName", createJobInfoCallable()));
    }

    @Test(expectedExceptions = JobNotRegisteredException.class)
    public void testAddingRunningConstraintForNotExistingJob() throws Exception {
        Set<String> constraint = new HashSet<String>();
        constraint.add("test");
        constraint.add("test2");
        jobService.addRunningConstraint(constraint);
    }

    @Test
    public void testAddingRunningConstraint() throws Exception {
        jobService.registerJob("test", createJobInfoCallable());
        jobService.registerJob("test2", createJobInfoCallable());
        Set<String> constraint = new HashSet<String>();
        constraint.add("test");
        constraint.add("test2");
        assertTrue(jobService.addRunningConstraint(constraint));
        assertFalse(jobService.addRunningConstraint(constraint));
    }


    @Test(expectedExceptions = JobNotRegisteredException.class)
    public void testQueuingJobWhichIsNotRegistered() throws Exception {
        jobService.queueJob("test");
    }

    @Test
    public void testQueuingJobWhichIsAlreadyQueued() throws Exception {
        when(jobInfoRepository.create("test", 1000, RunningState.QUEUED, Boolean.FALSE))
                .thenReturn(null);

        MockJobRunnable runnable = new MockJobRunnable(1000, true);
        jobService.registerJob("test", runnable);
        String id = jobService.queueJob("test");
        assertNull(id);
    }

    @Test
    public void testQueuingJob() throws Exception {
        when(jobInfoRepository.create("test", 1000, RunningState.QUEUED, Boolean.FALSE))
                .thenReturn("1234");

        MockJobRunnable runnable = new MockJobRunnable(1000, true);
        jobService.registerJob("test", runnable);
        String id = jobService.queueJob("test");
        assertNotNull(id);
        assertEquals("1234", id);
    }

    @Test(expectedExceptions = JobNotRegisteredException.class)
    public void testRemovingQueuedJobWhichIsNotRegistered() throws Exception {
        jobService.removeQueuedJob("test");
    }

    @Test
    public void testRemovingQueuedJobWhichIsNotQueued() throws Exception {
        when(jobInfoRepository.removeQueuedJob("test")).thenReturn(false);

        jobService.registerJob("test", createJobInfoCallable());
        assertFalse(jobService.removeQueuedJob("test"));
    }

    @Test
    public void testRemovingQueuedJob() throws Exception {
        when(jobInfoRepository.removeQueuedJob("test")).thenReturn(true);

        jobService.registerJob("test", createJobInfoCallable());
        assertTrue(jobService.removeQueuedJob("test"));
    }

    @Test
    public void testListingJobNamesRunningConstraintsAndCleaningThem() throws Exception {
        jobService.registerJob("test", createJobInfoCallable());
        jobService.registerJob("test2", createJobInfoCallable());
        Set<String> constraint = new HashSet<String>();
        constraint.add("test"); constraint.add("test2");
        jobService.addRunningConstraint(constraint);

        Set<String> jobNames = jobService.listJobNames();
        assertEquals(2, jobNames.size());
        Set<Set<String>> runningConstraints = jobService.listRunningConstraints();
        assertEquals(1, runningConstraints.size());

        jobService.clean();
        jobNames = jobService.listJobNames();
        assertEquals(0, jobNames.size());
        runningConstraints = jobService.listRunningConstraints();
        assertEquals(0, runningConstraints.size());
    }

    @Test
    public void testStopAllJobsJobRunning() throws Exception {
        when(jobInfoRepository.findRunningByName("jobName")).thenReturn(new JobInfo("jobName", InternetUtils.getHostName(), "bla", 60000L));

        jobService.registerJob("jobName", createJobInfoCallable());
        jobService.shutdownJobs();
        verify(jobInfoRepository).markAsFinished("jobName", ResultState.ERROR, "shutdownJobs called from executing host");
    }

    @Test
    public void testStopAllJobsJobRunningOnDifferentHost() throws Exception {
        when(jobInfoRepository.findRunningByName("jobName")).thenReturn(new JobInfo("jobName", "differentHost", "bla", 60000L));

        jobService.registerJob("jobName", createJobInfoCallable());
        jobService.shutdownJobs();
        verify(jobInfoRepository, never()).markAsFinished("jobName", ResultState.ERROR, "shutdownJobs called from executing host");
    }

    @Test
    public void testStopAllJobsNoRunning() throws Exception {
        jobService.registerJob("jobName", createJobInfoCallable());
        jobService.registerJob("jobName2", createJobInfoCallable());
        jobService.shutdownJobs();

        verify(jobInfoRepository, never()).markAsFinished("jobName", ResultState.ERROR, "shutdownJobs called from executing host");
        verify(jobInfoRepository, never()).markAsFinished("jobName2", ResultState.ERROR, "shutdownJobs called from executing host");
    }

    @Test
    public void testExecuteQueuedJobs() throws Exception {
        when(jobInfoRepository.activateQueuedJob("test")).thenReturn(true);
        when(jobInfoRepository.activateQueuedJob("test2")).thenReturn(false);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo("test", "bla", "bla", 1000), new JobInfo("test2", "bla", "bla", 1000)));
        MockJobRunnable runnable = new MockJobRunnable(1000, true);
        jobService.registerJob("test", runnable);
        jobService.registerJob("test2", createJobInfoCallable());

        jobService.executeQueuedJobs();
        verify(jobInfoRepository, times(1)).updateHostThreadInformation("test");
        verify(jobInfoRepository, times(0)).updateHostThreadInformation("test2");
        Thread.sleep(100);
        assertTrue(runnable.isExecuted());
        verify(jobInfoRepository, times(1)).markAsFinishedSuccessfully("test");
    }

    @Test
    public void testExecuteForcedQueuedJobs() throws Exception {
        when(jobInfoRepository.activateQueuedJob("test")).thenReturn(true);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo("test", "bla", "bla", 1000, RunningState.QUEUED, true, new HashMap<String, String>())));
        MockJobRunnable runnable = new MockJobRunnable(1000, false);
        jobService.registerJob("test", runnable);

        jobService.executeQueuedJobs();
        verify(jobInfoRepository, times(1)).updateHostThreadInformation("test");
        Thread.sleep(100);
        assertTrue(runnable.isExecuted());
        verify(jobInfoRepository, times(1)).markAsFinishedSuccessfully("test");
    }

    @Test
    public void testExecuteQueuedJobsFinishWithException() throws Exception {
        when(jobInfoRepository.activateQueuedJob("test")).thenReturn(true);
        when(jobInfoRepository.activateQueuedJob("test2")).thenReturn(false);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo("test", "bla", "bla", 1000), new JobInfo("test2", "bla", "bla", 1000)));
        JobRunnable runnable = new JobRunnable() {
            @Override
            public long getMaxExecutionTime() {
                return 1000;
            }

            @Override
            public boolean isExecutionNecessary() {
                return true;
            }

            @Override
            public void execute(JobLogger jobLogger) throws Exception {
                throw new Exception();
            }
        };
        jobService.registerJob("test", runnable);
        jobService.registerJob("test2", createJobInfoCallable());

        jobService.executeQueuedJobs();
        verify(jobInfoRepository, times(1)).updateHostThreadInformation("test");
        verify(jobInfoRepository, times(0)).updateHostThreadInformation("test2");
        Thread.sleep(100);
        verify(jobInfoRepository, times(1)).markAsFinishedWithException(anyString(), any(Exception.class));
    }

    @Test
    public void testExecuteQueuedJobsNoExecutionNecessary() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo("test", "bla", "bla", 1000)));
        when(jobInfoRepository.removeQueuedJob("test")).thenReturn(Boolean.TRUE);
        jobService.registerJob("test", new MockJobRunnable(1000, false));

        jobService.executeQueuedJobs();
        verify(jobInfoRepository, times(1)).removeQueuedJob("test");
    }

    @Test
    public void testExecuteQueuedJobsViolatesRunningConstraints() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo("test", "bla", "bla", 1000)));
        when(jobInfoRepository.hasRunningJob("test2")).thenReturn(Boolean.TRUE);

        jobService.registerJob("test", createJobInfoCallable());
        jobService.registerJob("test2", createJobInfoCallable());
        Set<String> constraint = new HashSet<String>();
        constraint.add("test"); constraint.add("test2");
        jobService.addRunningConstraint(constraint);
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJob(anyString());
        verify(jobInfoRepository, times(0)).removeQueuedJob(anyString());
    }

    @Test
    public void testExecuteQueuedJobAlreadyRunning() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo("test", "bla", "bla", 1000)));
        when(jobInfoRepository.hasRunningJob("test")).thenReturn(Boolean.TRUE);

        jobService.registerJob("test", createJobInfoCallable());
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJob(anyString());
        verify(jobInfoRepository, times(0)).removeQueuedJob(anyString());
    }

    /***
     *
     *  HELPER
     *
     */
    private JobRunnable createJobInfoCallable() {
        return new MockJobRunnable(0, true);
    }

    private class MockJobRunnable implements JobRunnable {

        private long maxExecutionTime;
        private boolean executionNecessary;
        private volatile boolean executed = false;

        private MockJobRunnable(long maxExecutionTime, boolean executionNecessary) {
            this.maxExecutionTime = maxExecutionTime;
            this.executionNecessary = executionNecessary;
        }

        @Override
        public long getMaxExecutionTime() {
            return maxExecutionTime;
        }

        @Override
        public boolean isExecutionNecessary() {
            return executionNecessary;
        }

        @Override
        public void execute(JobLogger jobLogger) throws Exception {
            executed = true;
        }

        public boolean isExecuted() {
            return executed;
        }

    }

}
