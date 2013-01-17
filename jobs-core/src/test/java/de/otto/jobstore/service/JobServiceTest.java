package de.otto.jobstore.service;

import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.common.util.InternetUtils;
import de.otto.jobstore.repository.JobDefinitionRepository;
import de.otto.jobstore.repository.JobInfoRepository;
import de.otto.jobstore.service.exception.*;
import org.springframework.test.util.ReflectionTestUtils;
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
    private JobDefinitionRepository jobDefinitionRepository;
    private RemoteJobExecutorService remoteJobExecutorService;
    private JobInfoService jobInfoService;

    private static final String JOB_NAME_01 = "test";
    private static final String JOB_NAME_02 = "test2";

    @BeforeMethod
    public void setUp() throws Exception {
        jobInfoRepository = mock(JobInfoRepository.class);
        jobDefinitionRepository = mock(JobDefinitionRepository.class);
        remoteJobExecutorService = mock(RemoteJobExecutorService.class);
        jobService = new JobService(jobDefinitionRepository, jobInfoRepository);
        jobInfoService = new JobInfoService(jobInfoRepository);
        when(jobDefinitionRepository.find(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName())).thenReturn(StoredJobDefinition.JOB_EXEC_SEMAPHORE);
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
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        when(jobDefinitionRepository.find(JOB_NAME_02)).thenReturn(createSimpleJd());
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
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
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
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        when(jobDefinitionRepository.find(JOB_NAME_02)).thenReturn(createSimpleJd());
        JobRunnable runnable = new AbstractLocalJobRunnable() {

            @Override
            public JobDefinition getJobDefinition() {
                return new AbstractLocalJobDefinition() {
                    @Override
                    public String getName() {
                        return JOB_NAME_01;
                    }

                    @Override
                    public long getTimeoutPeriod() {
                        return 1000;
                    }
                };
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
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_02));
        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01); constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJob(anyString());
    }

    @Test
    public void testExecuteQueuedJobsWhichIsDisabled() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L)));
        StoredJobDefinition jd = createSimpleJd(); jd.setDisabled(true);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(jd);

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJob(anyString());
    }

    @Test
    public void testExecuteQueuedJobAlreadyRunning() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L)));
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.TRUE);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJob(anyString());
    }

    @Test(expectedExceptions = JobAlreadyQueuedException.class)
    public void testExecuteJobWithSamePriorityOfJobWhichIsAlreadyQueued() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.QUEUED)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.QUEUED));
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void testExecuteJobWithHigherPriorityOfJobWhichIsAlreadyQueued() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.QUEUED)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.QUEUED));
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS,
                Collections.<String, String>emptyMap(), null))
                .thenReturn("1234");
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
    }

    @Test(expectedExceptions = JobExecutionNotNecessaryException.class)
    public void testExecuteJobWithSamePriorityOfJobWhichIsAlreadyRunning() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.RUNNING));
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void testExecuteJobWithHigherPriorityOfJobWhichIsAlreadyRunning() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS,
                Collections.<String, String>emptyMap(), null)).
                thenReturn("1234");
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.RUNNING));
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
    }

    @Test
    public void testExecuteJobForced() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS,
                Collections.<String,String>emptyMap(), null)).thenReturn("1234");
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(Boolean.TRUE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.FALSE);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        LocalMockJobRunnable runnable = new LocalMockJobRunnable(JOB_NAME_01, 0);

        jobService.registerJob(runnable);
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
        Thread.sleep(100);
        assertTrue(runnable.isExecuted());
        verify(jobInfoRepository, times(1)).activateQueuedJob(JOB_NAME_01);
    }

    @Test
    public void testExecuteJobForcedFailedWithException() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS,
                Collections.<String,String>emptyMap(), null)).thenReturn("1234");
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(Boolean.TRUE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.FALSE);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        JobRunnable runnable = new AbstractLocalJobRunnable() {

            @Override
            public JobDefinition getJobDefinition() {
                return new AbstractLocalJobDefinition() {
                    @Override
                    public String getName() {
                        return JOB_NAME_01;
                    }

                    @Override
                    public long getTimeoutPeriod() {
                        return 0;
                    }
                };
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

    @Test(expectedExceptions = JobExecutionDisabledException.class)
    public void testJobExecutedDisabled() throws Exception {
        reset(jobDefinitionRepository);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        StoredJobDefinition disabledJob = new StoredJobDefinition(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName(), 0, 0, false);
        disabledJob.setDisabled(true);
        when(jobDefinitionRepository.find(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName())).thenReturn(disabledJob);
        JobService jobServiceImpl = new JobService(jobDefinitionRepository, jobInfoRepository);

        jobServiceImpl.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobServiceImpl.executeJob(JOB_NAME_01);
    }

    @Test(expectedExceptions = JobExecutionDisabledException.class)
    public void testExecutingJobWhichIsDisabled() throws Exception {
        StoredJobDefinition jd = createSimpleJd();
        jd.setDisabled(true);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(jd);

        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void testPollRemoteJobsNoRemoteJobs() throws Exception {
        jobService.registerJob(createLocalJobRunnable(JOB_NAME_01));
        jobService.pollRemoteJobs();
        verify(jobInfoRepository, times(0)).findByNameAndRunningState(anyString(), any(RunningState.class));
    }

    @Test
    public void testPollRemoteJobsNoUpdateNecessary() throws Exception {
        jobService.registerJob(new RemoteMockJobRunnable(JOB_NAME_01, remoteJobExecutorService, jobInfoService, 0, 1000 * 60 * 60));
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(new JobInfo(JOB_NAME_01, "host", "thread", 1000L));
        jobService.pollRemoteJobs();
        verify(remoteJobExecutorService, times(0)).getStatus(any(URI.class));
    }

    @Test
    public void testPollRemoteJobsJobStillRunning() throws Exception {
        jobService.registerJob(new RemoteMockJobRunnable(JOB_NAME_01, remoteJobExecutorService, jobInfoService, 0, 0));
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L);
        job.putAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), "http://example.com");
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(job);
        List<String> logLines = Arrays.asList("test", "test1");
        when(remoteJobExecutorService.getStatus(any(URI.class)))
                .thenReturn(new RemoteJobStatus(RemoteJobStatus.Status.RUNNING, logLines, null, null));
        jobService.pollRemoteJobs();
        verify(jobInfoRepository, times(1)).appendLogLines(JOB_NAME_01, logLines);
    }

    @Test
    public void testPollRemoteJobsJobIsFinishedNotSuccessfully() throws Exception {
        jobService.registerJob(new RemoteMockJobRunnable(JOB_NAME_01, remoteJobExecutorService, jobInfoService, 0, 0));
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L);
        job.putAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), "http://example.com");
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(job);
        List<String> logLines = Arrays.asList("test", "test1");
        when(remoteJobExecutorService.getStatus(any(URI.class))).thenReturn(
                new RemoteJobStatus(RemoteJobStatus.Status.FINISHED, logLines, new RemoteJobResult(false, 1, "foo"), null));

        jobService.pollRemoteJobs();
        verify(jobInfoRepository, times(1)).markRunningAsFinished(JOB_NAME_01, ResultCode.FAILED, "foo");
    }

    @Test
    public void testPollRemoteJobsJobIsFinishedSuccessfully() throws Exception {
        RemoteMockJobRunnable runnable = new RemoteMockJobRunnable(JOB_NAME_01, remoteJobExecutorService, jobInfoService, 0, 0);
        jobService.registerJob(runnable);
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L);
        job.putAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), "http://example.com");
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(job);
        List<String> logLines = Arrays.asList("test", "test1");
        when(remoteJobExecutorService.getStatus(any(URI.class))).thenReturn(
                new RemoteJobStatus(RemoteJobStatus.Status.FINISHED, logLines, new RemoteJobResult(true, 0, "foo"), null));

        jobService.pollRemoteJobs();
        Thread.sleep(100);
        verify(jobInfoRepository, times(1)).markRunningAsFinished(JOB_NAME_01, ResultCode.SUCCESSFUL, "foo");
        assertEquals(ResultCode.SUCCESSFUL, runnable.afterSuccessContext.getResultCode());
    }

    @Test
    public void testPollRemoteJobsJobIsFinishedSuccessfullyAfterExecutionException() throws Exception {
        RemoteMockJobRunnable runnable = new RemoteMockJobRunnable(JOB_NAME_01, remoteJobExecutorService, jobInfoService, 0, 0);
        runnable.throwExceptionInAfterExecution = true;
        jobService.registerJob(runnable);
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L);
        job.putAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), "http://example.com");
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(job);
        List<String> logLines = Arrays.asList("test", "test1");
        when(remoteJobExecutorService.getStatus(any(URI.class))).thenReturn(
                new RemoteJobStatus(RemoteJobStatus.Status.FINISHED, logLines, new RemoteJobResult(true, 0, "foo"), null));

        jobService.pollRemoteJobs();
        Thread.sleep(100);
        verify(jobInfoRepository, times(1)).markRunningAsFinishedWithException(anyString(), any(Throwable.class));
        assertEquals(ResultCode.SUCCESSFUL, runnable.afterSuccessContext.getResultCode());
    }

    @Test
    public void testJobDoesRequireUpdate() throws Exception {
        Date dt = new Date();
        Date lastModification = new Date(dt.getTime() - 60L * 1000L);
        long currentTime = dt.getTime();
        long pollingInterval = 30 * 1000L;
        boolean requiresUpdate = ReflectionTestUtils.invokeMethod(jobService, "jobRequiresUpdate", lastModification, currentTime, pollingInterval);
        assertTrue(requiresUpdate);
    }

    @Test
    public void testJobDoesNotRequireUpdate() throws Exception {
        Date dt = new Date();
        Date lastModification = new Date(dt.getTime() - 60L * 1000L);
        long currentTime = dt.getTime();
        long pollingInterval = 90 * 1000L;
        boolean requiresUpdate = ReflectionTestUtils.invokeMethod(jobService, "jobRequiresUpdate", lastModification, currentTime, pollingInterval);
        assertFalse(requiresUpdate);
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
        public JobExecutionContext afterSuccessContext = null;
        public boolean throwExceptionInAfterExecution = false;

        // TODO: same as in JobServiceIntegrationTest
        private RemoteMockJobRunnable(String name, RemoteJobExecutorService rjes, JobInfoService jis, long maxExecutionTime, long pollingInterval) {
            super(rjes, jis);
            this.name = name;
            this.maxExecutionTime = maxExecutionTime;
            this.pollingInterval = pollingInterval;
        }

        @Override
        public JobDefinition getJobDefinition() {
            return new AbstractRemoteJobDefinition() {
                @Override
                public String getName() {
                    return name;
                }

                @Override
                public long getTimeoutPeriod() {
                    return maxExecutionTime;
                }

                @Override
                public long getPollingInterval() {
                    return pollingInterval;
                }
            };
        }

        @Override
        public Map<String, String> getParameters() {
            return null;
        }

        @Override
        public void afterExecution(JobExecutionContext context) throws JobException {
            afterSuccessContext = context;
            if (throwExceptionInAfterExecution) {
                throw new JobExecutionException("bar");
            }
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
        public JobDefinition getJobDefinition() {
            return new AbstractLocalJobDefinition() {
                @Override
                public String getName() {
                    return name;
                }

                @Override
                public long getTimeoutPeriod() {
                    return maxExecutionTime;
                }
            };
        }

        @Override
        public void execute(JobExecutionContext executionContext) throws JobExecutionException {
            executionContext.setResultCode(ResultCode.SUCCESSFUL);
            executed = true;
        }

        public boolean isExecuted() {
            return executed;
        }

    }

    private JobInfo createJobInfo(String name, JobExecutionPriority executionPriority, RunningState runningState) {
        return new JobInfo(name, "test", "test", 1000L, runningState, executionPriority, null);
    }

    private StoredJobDefinition createSimpleJd() {
        return new StoredJobDefinition("foo", 0, 0, false) ;
    }

}
