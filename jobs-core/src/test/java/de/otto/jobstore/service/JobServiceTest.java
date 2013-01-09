package de.otto.jobstore.service;

import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.repository.JobInfoRepository;
import de.otto.jobstore.service.exception.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class JobServiceTest {

    private JobService jobService;
    private JobInfoRepository jobInfoRepository;
    private RemoteJobExecutorService remoteJobExecutorService;

    private static final String JOB_NAME_01 = "test";
    private static final String JOB_NAME_02 = "test2";

    @BeforeMethod
    public void setUp() throws Exception {
        jobInfoRepository = mock(JobInfoRepository.class);
        remoteJobExecutorService = mock(RemoteJobExecutorService.class);
        jobService = new JobService(jobInfoRepository, remoteJobExecutorService);
    }

    @Test
    public void testRegisteringJob() throws Exception {
        assertTrue(jobService.registerJob(createLocalJobRunnable(JOB_NAME_01)));
        assertFalse(jobService.registerJob(createLocalJobRunnable(JOB_NAME_01)));
    }

    @Test(expectedExceptions = JobNotRegisteredException.class)
    public void testAddingRunningConstraintForNotExistingJob() throws Exception {
        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01);
        constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);
    }

    @Test
    public void testAddingRunningConstraint() throws Exception {
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_02));
        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01);
        constraint.add(JOB_NAME_02);
        assertTrue(jobService.addRunningConstraint(constraint));
        assertFalse(jobService.addRunningConstraint(constraint));
    }

    @Test
    public void testListingJobNamesRunningConstraintsAndCleaningThem() throws Exception {
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_02));
        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01); constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);

        Collection<String> jobNames = jobService.listJobNames();
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
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).thenReturn(new JobInfo(JOB_NAME_01, InternetUtils.getHostName(), "bla", 60000L));

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.shutdownJobs();
        verify(jobInfoRepository).markRunningAsFinished(JOB_NAME_01, ResultCode.FAILED, "shutdownJobs called from executing host");
    }

    @Test
    public void testStopAllJobsJobRunningOnDifferentHost() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).thenReturn(new JobInfo(JOB_NAME_01, "differentHost", "bla", 60000L));

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.shutdownJobs();
        verify(jobInfoRepository, never()).markRunningAsFinished(JOB_NAME_01, ResultCode.FAILED, "shutdownJobs called from executing host");
    }

    @Test
    public void testStopAllJobsNoRunning() throws Exception {
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_02));
        jobService.shutdownJobs();

        verify(jobInfoRepository, never()).markRunningAsFinished("jobName", ResultCode.FAILED, "shutdownJobs called from executing host");
        verify(jobInfoRepository, never()).markRunningAsFinished("jobName2", ResultCode.FAILED, "shutdownJobs called from executing host");
    }

    @Test
    public void testExecuteQueuedJobs() throws Exception {
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(true);
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_02)).thenReturn(false);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L), new JobInfo(JOB_NAME_02, "bla", "bla", 1000L)));
        LocalMockJobRunnable runnable = new LocalMockJobRunnable(JOB_NAME_01,1000);
        jobService.registerJob(runnable);
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_02));

        jobService.executeQueuedJobs();
        verify(jobInfoRepository, times(1)).updateHostThreadInformation(JOB_NAME_01);
        verify(jobInfoRepository, times(0)).updateHostThreadInformation(JOB_NAME_02);
        Thread.sleep(100);
        assertTrue(runnable.isExecuted());
        verify(jobInfoRepository, times(1)).markRunningAsFinished(JOB_NAME_01, ResultCode.SUCCESSFUL, null);
    }

    @Test
    public void testExecuteForcedQueuedJobs() throws Exception {
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(true);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS, new HashMap<String, String>())));
        LocalMockJobRunnable runnable = new LocalMockJobRunnable(JOB_NAME_01,1000);
        jobService.registerJob(runnable);

        jobService.executeQueuedJobs();
        verify(jobInfoRepository, times(1)).updateHostThreadInformation(JOB_NAME_01);
        Thread.sleep(100);
        assertTrue(runnable.isExecuted());
        verify(jobInfoRepository, times(1)).markRunningAsFinished(JOB_NAME_01, ResultCode.SUCCESSFUL, null);
    }

    @Test
    public void testExecuteQueuedJobsFinishWithException() throws Exception {
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(true);
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_02)).thenReturn(false);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L), new JobInfo(JOB_NAME_02, "bla", "bla", 1000L)));
        JobRunnable runnable = new AbstractLocalJobRunnable() {

            @Override
            public String getName() {
                return JOB_NAME_01;
            }

            @Override
            public Map<String, String> getParameters() {
                return null;
            }

            @Override
            public long getMaxExecutionTime() {
                return 1000;
            }

            @Override
            public void execute(JobExecutionContext executionContext) throws JobExecutionException {
                throw new JobExecutionException("problem while executing");
            }
        };
        jobService.registerJob(runnable);
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_02));

        jobService.executeQueuedJobs();
        verify(jobInfoRepository, times(1)).updateHostThreadInformation(JOB_NAME_01);
        verify(jobInfoRepository, times(0)).updateHostThreadInformation(JOB_NAME_02);
        Thread.sleep(100);
        verify(jobInfoRepository, times(1)).markRunningAsFinishedWithException(anyString(), any(Exception.class));
    }

    @Test
    public void testExecuteQueuedJobsViolatesRunningConstraints() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L)));
        when(jobInfoRepository.hasJob(JOB_NAME_02, RunningState.RUNNING)).thenReturn(Boolean.TRUE);

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_02));
        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01); constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJob(anyString());
    }

    @Test
    public void testExecuteQueuedJobAlreadyRunning() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L)));
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.TRUE);

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJob(anyString());
    }

    @Test(expectedExceptions = JobAlreadyQueuedException.class)
    public void testExecuteJobWithSamePriorityOfJobWhichIsAlreadyQueued() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.QUEUED)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.QUEUED));

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void testExecuteJobWithHigherPriorityOfJobWhichIsAlreadyQueued() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.QUEUED)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.QUEUED));
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS, null, null))
                .thenReturn("1234");

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
    }

    @Test(expectedExceptions = JobExecutionNotNecessaryException.class)
    public void testExecuteJobWithSamePriorityOfJobWhichIsAlreadyRunning() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.RUNNING));

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void testExecuteJobWithHigherPriorityOfJobWhichIsAlreadyRunning() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS, null, null)).
                thenReturn("1234");
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.RUNNING));

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
    }

    @Test
    public void testExecuteJobWhichViolatesRunningConstraints() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null)).thenReturn("1234");
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_02, RunningState.RUNNING)).
                thenReturn(createJobInfo(JOB_NAME_02, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.RUNNING));

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_02));
        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01); constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);

        String id = jobService.executeJob(JOB_NAME_01);
        assertEquals("1234", id);
    }

    @Test
    public void testExecuteJobForced() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.RUNNING, JobExecutionPriority.IGNORE_PRECONDITIONS, null, null)).thenReturn("1234");
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.FALSE);
        LocalMockJobRunnable runnable = new LocalMockJobRunnable(JOB_NAME_01, 0);

        jobService.registerJob(runnable);
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
        Thread.sleep(100);
        assertTrue(runnable.isExecuted());
        verify(jobInfoRepository, times(1)).markRunningAsFinished(JOB_NAME_01, ResultCode.SUCCESSFUL, null);
    }

    @Test
    public void testExecuteJobForcedFailedWithException() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.RUNNING, JobExecutionPriority.IGNORE_PRECONDITIONS, null, null)).thenReturn("1234");
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.FALSE);
        JobRunnable runnable = new AbstractLocalJobRunnable() {

            @Override
            public String getName() {
                return JOB_NAME_01;
            }

            @Override
            public Map<String, String> getParameters() {
                return null;
            }

            @Override
            public long getMaxExecutionTime() {
                return 0;
            }

            @Override
            public void execute(JobExecutionContext executionContext) throws JobExecutionException {
                throw new JobExecutionException("problem while executing");
            }
        };

        jobService.registerJob(runnable);
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
        Thread.sleep(100);
        verify(jobInfoRepository, times(1)).markRunningAsFinishedWithException(anyString(), any(Exception.class));
    }

    @Test(expectedExceptions = JobAlreadyRunningException.class)
    public void testExecuteJobAndRunningFails() throws Exception {
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.FALSE);
        LocalMockJobRunnable runnable = new LocalMockJobRunnable(JOB_NAME_01, 0);

        jobService.registerJob(runnable);
        jobService.executeJob(JOB_NAME_01);
    }

    @Test(expectedExceptions = JobExecutionDisabledException.class)
    public void testJobExecutedDisabled() throws Exception {
        JobService jobServiceImpl = new JobService(jobInfoRepository, remoteJobExecutorService);
        jobServiceImpl.setExecutionEnabled(false);

        jobServiceImpl.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobServiceImpl.executeJob(JOB_NAME_01);
    }

    @Test
    public void testPollRemoteJobsNoRemoteJobs() throws Exception {
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.pollRemoteJobs();
        verify(jobInfoRepository, times(0)).findByNameAndRunningState(anyString(), any(RunningState.class));
    }

    @Test
    public void testPollRemoteJobsNoUpdateNecessary() throws Exception {
        jobService.registerJob(new RemoteMockJobRunnable(JOB_NAME_01, 0, 1000 * 60 * 60));
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(new JobInfo(JOB_NAME_01, "host", "thread", 1000L));
        jobService.pollRemoteJobs();
        verify(remoteJobExecutorService, times(0)).getStatus(any(URI.class));
    }

    @Test
    public void testPollRemoteJobsJobStillRunning() throws Exception {
        jobService.registerJob(new RemoteMockJobRunnable(JOB_NAME_01, 0, 0));
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L);
        job.putAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), "http://example.com");
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(job);
        List<String> logLines = Arrays.asList("test", "test1");
        when(remoteJobExecutorService.getStatus(any(URI.class))).thenReturn(
                new RemoteJobStatus(RemoteJobStatus.Status.RUNNING, logLines, null));
        jobService.pollRemoteJobs();
        verify(jobInfoRepository, times(1)).setLogLines(JOB_NAME_01, logLines);
    }

    /***
     *  HELPER
     */
    private JobRunnable createLocalJobRunnable(String name) {
        return new LocalMockJobRunnable(name, 0);
    }

    private class RemoteMockJobRunnable extends AbstractRemoteJobRunnable {

        private String name;
        private long maxExecutionTime;
        private long pollingInterval;

        private RemoteMockJobRunnable(String name, long maxExecutionTime, long pollingInterval) {
            super(remoteJobExecutorService);
            this.name = name;
            this.maxExecutionTime = maxExecutionTime;
            this.pollingInterval = pollingInterval;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public long getMaxExecutionTime() {
            return maxExecutionTime;
        }

        @Override
        public long getPollingInterval() {
            return pollingInterval;
        }

        @Override
        public Map<String, String> getParameters() {
            return null;
        }

        @Override
        public void execute(JobExecutionContext executionContext) throws JobExecutionException {
            executionContext.setResultCode(ResultCode.SUCCESSFUL);
        }

    }


    private class LocalMockJobRunnable extends AbstractLocalJobRunnable {

        private String name;
        private long maxExecutionTime;
        private volatile boolean executed = false;

        private LocalMockJobRunnable(String name, long maxExecutionTime) {
            this.name = name;
            this.maxExecutionTime = maxExecutionTime;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Map<String, String> getParameters() {
            return null;
        }

        @Override
        public long getMaxExecutionTime() {
            return maxExecutionTime;
        }

        @Override
        public void execute(JobExecutionContext executionContext) throws JobExecutionException {
            executed = true;
            executionContext.setRunningState(RunningState.FINISHED);
            executionContext.setResultCode(ResultCode.SUCCESSFUL);
        }

        public boolean isExecuted() {
            return executed;
        }

    }

    private JobInfo createJobInfo(String name, JobExecutionPriority executionPriority, RunningState runningState) {
        return new JobInfo(name, "test", "test", 1000L, runningState);
    }

}
