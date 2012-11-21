package de.otto.jobstore.service.impl;

import de.otto.jobstore.common.*;
import de.otto.jobstore.repository.api.JobInfoRepository;
import de.otto.jobstore.service.api.JobService;
import de.otto.jobstore.service.exception.JobAlreadyQueuedException;
import de.otto.jobstore.service.exception.JobAlreadyRunningException;
import de.otto.jobstore.service.exception.JobExecutionNotNecessaryException;
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

    private static final String JOB_NAME_01 = "test";
    private static final String JOB_NAME_02 = "test2";

    @BeforeMethod
    public void setUp() throws Exception {
        jobInfoRepository = mock(JobInfoRepository.class);
        jobService = new JobServiceImpl(jobInfoRepository, true);
    }

    @Test
    public void testRegisteringJob() throws Exception {
        assertTrue(jobService.registerJob(JOB_NAME_01, createJobInfoCallable()));
        assertFalse(jobService.registerJob(JOB_NAME_01, createJobInfoCallable()));
    }

    @Test(expectedExceptions = JobNotRegisteredException.class)
    public void testAddingRunningConstraintForNotExistingJob() throws Exception {
        Set<String> constraint = new HashSet<String>();
        constraint.add(JOB_NAME_01);
        constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);
    }

    @Test
    public void testAddingRunningConstraint() throws Exception {
        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        jobService.registerJob(JOB_NAME_02, createJobInfoCallable());
        Set<String> constraint = new HashSet<String>();
        constraint.add(JOB_NAME_01);
        constraint.add(JOB_NAME_02);
        assertTrue(jobService.addRunningConstraint(constraint));
        assertFalse(jobService.addRunningConstraint(constraint));
    }

    @Test
    public void testListingJobNamesRunningConstraintsAndCleaningThem() throws Exception {
        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        jobService.registerJob(JOB_NAME_02, createJobInfoCallable());
        Set<String> constraint = new HashSet<String>();
        constraint.add(JOB_NAME_01); constraint.add(JOB_NAME_02);
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
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING.name())).thenReturn(new JobInfo(JOB_NAME_01, InternetUtils.getHostName(), "bla", 60000L));

        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        jobService.shutdownJobs();
        verify(jobInfoRepository).markAsFinished(JOB_NAME_01, ResultState.ERROR, "shutdownJobs called from executing host");
    }

    @Test
    public void testStopAllJobsJobRunningOnDifferentHost() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING.name())).thenReturn(new JobInfo(JOB_NAME_01, "differentHost", "bla", 60000L));

        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        jobService.shutdownJobs();
        verify(jobInfoRepository, never()).markAsFinished(JOB_NAME_01, ResultState.ERROR, "shutdownJobs called from executing host");
    }

    @Test
    public void testStopAllJobsNoRunning() throws Exception {
        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        jobService.registerJob(JOB_NAME_02, createJobInfoCallable());
        jobService.shutdownJobs();

        verify(jobInfoRepository, never()).markAsFinished("jobName", ResultState.ERROR, "shutdownJobs called from executing host");
        verify(jobInfoRepository, never()).markAsFinished("jobName2", ResultState.ERROR, "shutdownJobs called from executing host");
    }

    @Test
    public void testExecuteQueuedJobs() throws Exception {
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(true);
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_02)).thenReturn(false);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000), new JobInfo(JOB_NAME_02, "bla", "bla", 1000)));
        MockJobRunnable runnable = new MockJobRunnable(1000, true);
        jobService.registerJob(JOB_NAME_01, runnable);
        jobService.registerJob(JOB_NAME_02, createJobInfoCallable());

        jobService.executeQueuedJobs();
        verify(jobInfoRepository, times(1)).updateHostThreadInformation(JOB_NAME_01);
        verify(jobInfoRepository, times(0)).updateHostThreadInformation(JOB_NAME_02);
        Thread.sleep(100);
        assertTrue(runnable.isExecuted());
        verify(jobInfoRepository, times(1)).markAsFinishedSuccessfully(JOB_NAME_01);
    }

    @Test
    public void testExecuteForcedQueuedJobs() throws Exception {
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(true);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000, RunningState.QUEUED, true, new HashMap<String, String>())));
        MockJobRunnable runnable = new MockJobRunnable(1000, false);
        jobService.registerJob(JOB_NAME_01, runnable);

        jobService.executeQueuedJobs();
        verify(jobInfoRepository, times(1)).updateHostThreadInformation(JOB_NAME_01);
        Thread.sleep(100);
        assertTrue(runnable.isExecuted());
        verify(jobInfoRepository, times(1)).markAsFinishedSuccessfully(JOB_NAME_01);
    }

    @Test
    public void testExecuteQueuedJobsFinishWithException() throws Exception {
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(true);
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_02)).thenReturn(false);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000), new JobInfo(JOB_NAME_02, "bla", "bla", 1000)));
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
        jobService.registerJob(JOB_NAME_01, runnable);
        jobService.registerJob(JOB_NAME_02, createJobInfoCallable());

        jobService.executeQueuedJobs();
        verify(jobInfoRepository, times(1)).updateHostThreadInformation(JOB_NAME_01);
        verify(jobInfoRepository, times(0)).updateHostThreadInformation(JOB_NAME_02);
        Thread.sleep(100);
        verify(jobInfoRepository, times(1)).markAsFinishedWithException(anyString(), any(Exception.class));
    }

    @Test
    public void testExecuteQueuedJobsNoExecutionNecessary() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000)));
        when(jobInfoRepository.abortJob(anyString(), anyString())).thenReturn(Boolean.TRUE);
        jobService.registerJob(JOB_NAME_01, new MockJobRunnable(1000, false));

        jobService.executeQueuedJobs();
        verify(jobInfoRepository, times(1)).abortJob(anyString(), anyString());
    }

    @Test
    public void testExecuteQueuedJobsViolatesRunningConstraints() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000)));
        when(jobInfoRepository.hasJob(JOB_NAME_02, RunningState.RUNNING.name())).thenReturn(Boolean.TRUE);

        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        jobService.registerJob(JOB_NAME_02, createJobInfoCallable());
        Set<String> constraint = new HashSet<String>();
        constraint.add(JOB_NAME_01); constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJob(anyString());
        verify(jobInfoRepository, times(0)).abortJob(anyString(), anyString());
    }

    @Test
    public void testExecuteQueuedJobAlreadyRunning() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000)));
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING.name())).thenReturn(Boolean.TRUE);

        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJob(anyString());
        verify(jobInfoRepository, times(0)).abortJob(anyString(), anyString());
    }

    @Test(expectedExceptions = JobAlreadyQueuedException.class)
    public void testExecuteJobWhichIsAlreadyQueued() throws Exception {
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED.name())).thenReturn(Boolean.TRUE);

        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void testExecuteJobWhichIsAlreadyRunning() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.QUEUED, false)).thenReturn("1234");
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED.name())).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING.name())).thenReturn(Boolean.TRUE);

        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        String id = jobService.executeJob(JOB_NAME_01);
        assertEquals("1234", id);
    }

    @Test(expectedExceptions = JobAlreadyQueuedException.class)
    public void testExecuteJobWhichIsAlreadyRunningAndQueuingFails() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.QUEUED, false)).thenReturn(null);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED.name())).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING.name())).thenReturn(Boolean.TRUE);

        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void testExecuteJobWhichViolatesRunningConstraints() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.QUEUED, false)).thenReturn("1234");
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED.name())).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING.name())).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_02, RunningState.RUNNING.name())).thenReturn(Boolean.TRUE);

        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        jobService.registerJob(JOB_NAME_02, createJobInfoCallable());
        Set<String> constraint = new HashSet<String>();
        constraint.add(JOB_NAME_01); constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);

        String id = jobService.executeJob(JOB_NAME_01);
        assertEquals("1234", id);
    }

    @Test(expectedExceptions = JobAlreadyQueuedException.class)
    public void testExecuteJobWhichViolatesRunningConstraintsAndQueuingFails() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.QUEUED, false)).thenReturn(null);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED.name())).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING.name())).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_02, RunningState.RUNNING.name())).thenReturn(Boolean.TRUE);

        jobService.registerJob(JOB_NAME_01, createJobInfoCallable());
        jobService.registerJob(JOB_NAME_02, createJobInfoCallable());
        Set<String> constraint = new HashSet<String>();
        constraint.add(JOB_NAME_01); constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);

        jobService.executeJob(JOB_NAME_01);
    }

    @Test(expectedExceptions = JobExecutionNotNecessaryException.class)
    public void testExecuteJobWhichIsNotNecessary() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.QUEUED, false)).thenReturn(null);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED.name())).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING.name())).thenReturn(Boolean.FALSE);

        jobService.registerJob(JOB_NAME_01, new MockJobRunnable(0, false));
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void testExecuteJobForced() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.RUNNING, true)).thenReturn("1234");
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED.name())).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING.name())).thenReturn(Boolean.FALSE);
        MockJobRunnable runnable = new MockJobRunnable(0, false);

        jobService.registerJob(JOB_NAME_01, runnable);
        String id = jobService.executeJob(JOB_NAME_01, true);
        assertEquals("1234", id);
        Thread.sleep(100);
        assertTrue(runnable.isExecuted());
        verify(jobInfoRepository, times(1)).markAsFinishedSuccessfully(JOB_NAME_01);
    }

    @Test
    public void testExecuteJobForcedFailedWithException() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.RUNNING, true)).thenReturn("1234");
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED.name())).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING.name())).thenReturn(Boolean.FALSE);
        JobRunnable runnable = new JobRunnable() {
            @Override
            public long getMaxExecutionTime() {
                return 0;
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

        jobService.registerJob(JOB_NAME_01, runnable);
        String id = jobService.executeJob(JOB_NAME_01, true);
        assertEquals("1234", id);
        Thread.sleep(100);
        verify(jobInfoRepository, times(1)).markAsFinishedWithException(anyString(), any(Exception.class));
    }

    @Test(expectedExceptions = JobAlreadyRunningException.class)
    public void testExecuteJobAndRunningFails() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.RUNNING, true)).thenReturn(null);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED.name())).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING.name())).thenReturn(Boolean.FALSE);
        MockJobRunnable runnable = new MockJobRunnable(0, true);

        jobService.registerJob(JOB_NAME_01, runnable);
        jobService.executeJob(JOB_NAME_01);
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