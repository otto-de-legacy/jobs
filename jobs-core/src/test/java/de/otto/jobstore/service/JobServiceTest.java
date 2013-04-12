package de.otto.jobstore.service;

import de.otto.jobstore.TestSetup;
import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.common.util.InternetUtils;
import de.otto.jobstore.repository.JobDefinitionRepository;
import de.otto.jobstore.repository.JobInfoRepository;
import de.otto.jobstore.service.exception.*;
import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;
import org.bson.types.ObjectId;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
    private JobExecutionException jobExecutionException;

    @BeforeMethod
    public void setUp() throws Exception {
        jobInfoRepository = mock(JobInfoRepository.class);
        jobDefinitionRepository = mock(JobDefinitionRepository.class);
        remoteJobExecutorService = mock(RemoteJobExecutorService.class);
        jobService = new JobService(jobDefinitionRepository, jobInfoRepository);
        jobInfoService = new JobInfoService(jobInfoRepository);
        when(jobDefinitionRepository.find(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName())).thenReturn(StoredJobDefinition.JOB_EXEC_SEMAPHORE);
        jobService.awaitTerminationSeconds=1;
        jobService.desynchronize = false;
        jobService.startup();
    }

    @Test
    public void testRegisteringJob() throws Exception {
        assertTrue(jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0)));
        assertFalse(jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0)));
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
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_02, 0));
        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01);
        constraint.add(JOB_NAME_02);
        assertTrue(jobService.addRunningConstraint(constraint));
        assertFalse(jobService.addRunningConstraint(constraint));
    }

    private synchronized void printIt(String msg) {
        System.out.println(Thread.currentThread().getName()+":"+ msg);
    }

    @Test
    public void testMultipleThreadsOnRunningConstraints() throws Throwable {

        final JobRunnable job1 = TestSetup.localJobRunnable(JOB_NAME_01, 0);
        final JobRunnable job2 = TestSetup.localJobRunnable(JOB_NAME_02, 0);

        Set<String> constraint = new HashSet<>();
        constraint.add(job1.getJobDefinition().getName());
        constraint.add(job2.getJobDefinition().getName());

        StoredJobDefinition jd = createSimpleJd();
        when(jobDefinitionRepository.find(anyString())).thenReturn(jd);

        jobService.registerJob(job1);
        jobService.registerJob(job2);
        jobService.addRunningConstraint(constraint);

        // first do nothing, second should throw exception
        final AtomicInteger countUpdateHostThreadInformation = new AtomicInteger(0);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("updateHostThreadInformation");
                if(countUpdateHostThreadInformation.incrementAndGet() > 1) {
                    throw new RuntimeException("called too often");
                }
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        }).when(jobInfoRepository).updateHostThreadInformation(anyString());

        final AtomicInteger state1 = new AtomicInteger(0);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("hasJob.QUEUED.1");
                return state1.get() == 0;
            }
        });
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("hasJob.RUNNING.1");
                return state1.get() == 1;
            }
        });
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("activateQueuedJob.1");
                return state1.compareAndSet(0,1);
            }
        });
        when(jobInfoRepository.deactivateRunningJob(JOB_NAME_01)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("deactivateRunningJob.1");
                return state1.compareAndSet(1,0);
            }
        });

        final AtomicInteger state2 = new AtomicInteger(0);
        when(jobInfoRepository.hasJob(JOB_NAME_02, RunningState.QUEUED)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("hasJob.QUEUED.2");
                return state2.get() == 0;
            }
        });
        when(jobInfoRepository.hasJob(JOB_NAME_02, RunningState.RUNNING)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("hasJob.RUNNING.2");
                return state2.get() == 1;
            }
        });
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_02)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("activateQueuedJob.2");
                return state2.compareAndSet(0,1);
            }
        });
        when(jobInfoRepository.deactivateRunningJob(JOB_NAME_02)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("deactivateRunningJob.2");
                return state2.compareAndSet(1,0);

            }
        });

        TestFramework.runOnce(new MultithreadedTestCase() {

            public void thread1() throws Exception {
                jobService.executeQueuedJob(job1,"id1", JobExecutionPriority.CHECK_PRECONDITIONS);
            }

            public void thread2() throws Exception {
                jobService.executeQueuedJob(job2,"id2", JobExecutionPriority.CHECK_PRECONDITIONS);
            }

        });

    }

    @Test
    public void testListingJobNamesRunningConstraintsAndCleaningThem() throws Exception {
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_02, 0));
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
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        JobInfo job = new JobInfo(JOB_NAME_01, InternetUtils.getHostName(), "bla", 60000L, 60000L);
        ReflectionTestUtils.invokeMethod(job, "addProperty", JobInfoProperty.ID, new ObjectId());
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).thenReturn(job);

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.shutdownJobs();
        verify(jobInfoRepository).markAsFinished(job.getId(), ResultCode.ABORTED, "shutdownJobs called from executing host");
    }

    @Test
    public void testStopAllJobsJobRunningOnDifferentHost() throws Exception {
        JobInfo jobInfo = new JobInfo(JOB_NAME_01, "differentHost", "bla", 60000L, 60000L);
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.ID, new ObjectId());
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).thenReturn(jobInfo);

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.shutdownJobs();
        verify(jobInfoRepository, never()).markAsFinished(jobInfo.getId(), ResultCode.FAILED, "shutdownJobs called from executing host");
    }

    @Test
    public void testStopAllJobsNoRunning() throws Exception {
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_02, 0));
        jobService.shutdownJobs();

        verify(jobInfoRepository, never()).markAsFinished(anyString(), any(ResultCode.class), anyString());
    }

    @Test
    public void testExecuteQueuedJobs() throws Exception {
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(true);
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_02)).thenReturn(false);
        JobInfo jobInfo = new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L);
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.ID, new ObjectId());
        final JobInfo jobInfo2 = new JobInfo(JOB_NAME_02, "bla", "bla", 1000L, 1000L);
        ReflectionTestUtils.invokeMethod(jobInfo2, "addProperty", JobInfoProperty.ID, new ObjectId());
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(jobInfo, jobInfo2));
        TestSetup.LocalMockJobRunnable runnable = TestSetup.localJobRunnable(JOB_NAME_01,1000);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        when(jobDefinitionRepository.find(JOB_NAME_02)).thenReturn(createSimpleJd());
        jobService.registerJob(runnable);
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_02, 0));

        jobService.executeQueuedJobs();
        Thread.sleep(500);
        verify(jobInfoRepository, times(1)).updateHostThreadInformation(jobInfo.getId());
        verify(jobInfoRepository, times(0)).updateHostThreadInformation(jobInfo2.getId());
        assertTrue(runnable.isExecuted());
        verify(jobInfoRepository, times(1)).markAsFinished(jobInfo.getId(), ResultCode.SUCCESSFUL, null);
    }

    @Test
    public void testExecuteForcedQueuedJobs() throws Exception {
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(true);
        JobInfo jobInfo = new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS, new HashMap<String, String>());
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.ID, new ObjectId());
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(Arrays.asList(jobInfo));
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        TestSetup.LocalMockJobRunnable runnable = TestSetup.localJobRunnable(JOB_NAME_01, 1000);

        jobService.registerJob(runnable);

        jobService.executeQueuedJobs();
        Thread.sleep(500);
        verify(jobInfoRepository, times(1)).updateHostThreadInformation(jobInfo.getId());
        assertTrue(runnable.isExecuted());
        verify(jobInfoRepository, times(1)).markAsFinished(jobInfo.getId(), ResultCode.SUCCESSFUL, null);
    }

    @Test
    public void testExecuteQueuedJobsFinishWithException() throws Exception {
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(true);
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_02)).thenReturn(false);
        JobInfo jobInfo = new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L);
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.ID, new ObjectId());
        final JobInfo jobInfo2 = new JobInfo(JOB_NAME_02, "bla", "bla", 1000L, 1000L);
        ReflectionTestUtils.invokeMethod(jobInfo2, "addProperty", JobInfoProperty.ID, new ObjectId());
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(jobInfo, jobInfo2));
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        when(jobDefinitionRepository.find(JOB_NAME_02)).thenReturn(createSimpleJd());
        final JobExecutionException exception = new JobExecutionException("problem while executing");
        JobRunnable runnable = TestSetup.localJobRunnable(JOB_NAME_01, 1000, exception);
        jobService.registerJob(runnable);
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_02, 0));

        jobService.executeQueuedJobs();
        Thread.sleep(500);
        verify(jobInfoRepository, times(1)).updateHostThreadInformation(jobInfo.getId());
        verify(jobInfoRepository, times(0)).updateHostThreadInformation(jobInfo2.getId());
        verify(jobInfoRepository, times(1)).markAsFinished(jobInfo.getId(), exception);
    }

    @Test
    public void testExecuteQueuedJobsViolatesRunningConstraints() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L)));
        when(jobInfoRepository.hasJob(JOB_NAME_02, RunningState.RUNNING)).thenReturn(Boolean.TRUE);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(true);

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_02, 0));
        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01);
        constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).updateHostThreadInformation(anyString());
    }

    @Test
    public void testExecuteQueuedJobsWhichIsDisabled() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L)));
        StoredJobDefinition jd = createSimpleJd(); jd.setDisabled(true);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(jd);

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJob(anyString());
    }

    @Test
    public void testExecuteQueuedJobAlreadyRunning() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L)));
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.TRUE);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJob(anyString());
    }

    @Test(expectedExceptions = JobAlreadyQueuedException.class)
    public void testExecuteJobWithSamePriorityOfJobWhichIsAlreadyQueued() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.QUEUED)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.QUEUED));
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void testExecuteJobWithHigherPriorityOfJobWhichIsAlreadyQueued() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.QUEUED)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.QUEUED));
        when(jobInfoRepository.create(JOB_NAME_01, 0, 0, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS,
                Collections.<String, String>emptyMap(), null))
                .thenReturn("1234");
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
    }

    @Test(expectedExceptions = JobExecutionNotNecessaryException.class)
    public void testExecuteJobWithSamePriorityOfJobWhichIsAlreadyRunning() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.RUNNING));
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void testExecuteJobWithHigherPriorityOfJobWhichIsAlreadyRunning() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, 0, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS,
                Collections.<String, String>emptyMap(), null)).
                thenReturn("1234");
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.RUNNING));
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
    }

    @Test
    public void testExecuteJobForced() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, 0, RunningState.RUNNING, JobExecutionPriority.IGNORE_PRECONDITIONS,
                Collections.<String,String>emptyMap(), null)).thenReturn("1234");
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(Boolean.TRUE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.FALSE);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        TestSetup.LocalMockJobRunnable runnable = TestSetup.localJobRunnable(JOB_NAME_01, 0);

        jobService.registerJob(runnable);
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
        Thread.sleep(500);
        assertTrue(runnable.isExecuted());
    }

    @Test
    public void testExecuteJobForcedFailedWithException() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, 0, RunningState.RUNNING, JobExecutionPriority.IGNORE_PRECONDITIONS,
                Collections.<String,String>emptyMap(), null)).thenReturn("1234");
        when(jobInfoRepository.activateQueuedJob(JOB_NAME_01)).thenReturn(Boolean.TRUE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.FALSE);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        final JobExecutionException exception = new JobExecutionException("problem while executing");
        JobRunnable runnable = TestSetup.localJobRunnable(JOB_NAME_01, 0, exception);

        jobService.registerJob(runnable);
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
        Thread.sleep(500);
        verify(jobInfoRepository, times(1)).markAsFinished(id, exception);
    }

    @Test(expectedExceptions = JobExecutionDisabledException.class)
    public void testJobExecutedDisabled() throws Exception {
        reset(jobDefinitionRepository);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        StoredJobDefinition disabledJob = new StoredJobDefinition(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName(), 0, 0, false, false);
        disabledJob.setDisabled(true);
        when(jobDefinitionRepository.find(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName())).thenReturn(disabledJob);
        JobService jobServiceImpl = new JobService(jobDefinitionRepository, jobInfoRepository);

        jobServiceImpl.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobServiceImpl.executeJob(JOB_NAME_01);
    }

    @Test(expectedExceptions = JobExecutionDisabledException.class)
    public void testExecutingJobWhichIsDisabled() throws Exception {
        StoredJobDefinition jd = createSimpleJd();
        jd.setDisabled(true);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(jd);

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.executeJob(JOB_NAME_01);
    }

    @Test
    public void testPollRemoteJobsNoRemoteJobs() throws Exception {
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.pollRemoteJobs();
        verify(jobInfoRepository, times(0)).findByNameAndRunningState(anyString(), any(RunningState.class));
    }

    @Test
    public void testPollRemoteJobsNoUpdateNecessary() throws Exception {
        jobService.registerJob(new RemoteMockJobRunnable(JOB_NAME_01, remoteJobExecutorService, jobInfoService, 0, 1000 * 60 * 60));
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(new JobInfo(JOB_NAME_01, "host", "thread", 1000L, 1000L));
        jobService.pollRemoteJobs();
        verify(remoteJobExecutorService, times(0)).getStatus(any(URI.class));
    }

    @Test
    public void testPollRemoteJobsJobStillRunning() throws Exception {
        jobService.registerJob(new RemoteMockJobRunnable(JOB_NAME_01, remoteJobExecutorService, jobInfoService, 0, 0));
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L, 1000L);
        job.putAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), "http://example.com");
        final ObjectId id = new ObjectId();
        ReflectionTestUtils.invokeMethod(job, "addProperty", JobInfoProperty.ID, id);
        when(jobInfoRepository.findById(id.toString())).thenReturn(job);
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).thenReturn(job);

        List<String> logLines = Arrays.asList("test", "test1");
        when(remoteJobExecutorService.getStatus(any(URI.class)))
                .thenReturn(new RemoteJobStatus(RemoteJobStatus.Status.RUNNING, logLines, null, null));
        jobService.pollRemoteJobs();
        verify(jobInfoRepository, times(1)).appendLogLines(job.getId(), logLines);
    }

    @Test(enabled = false)
    public void testPollRemoteJobsJobIsFinishedNotSuccessfully() throws Exception {
        jobService.registerJob(new RemoteMockJobRunnable(JOB_NAME_01, remoteJobExecutorService, jobInfoService, 0, 0));
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L, 1000L);
        job.putAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), "http://example.com");
        ReflectionTestUtils.invokeMethod(job, "addProperty", JobInfoProperty.ID, new ObjectId());
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(job);
        List<String> logLines = Arrays.asList("test", "test1");
        when(remoteJobExecutorService.getStatus(any(URI.class))).thenReturn(
                new RemoteJobStatus(RemoteJobStatus.Status.FINISHED, logLines, new RemoteJobResult(false, 1, "foo"), null));

        jobService.pollRemoteJobs();
        verify(jobInfoRepository, times(1)).markAsFinished(job.getId(), ResultCode.FAILED, "foo");
    }

    @Test
    public void testPollRemoteJobsJobIsFinishedSuccessfully() throws Exception {
        RemoteMockJobRunnable runnable = new RemoteMockJobRunnable(JOB_NAME_01, remoteJobExecutorService, jobInfoService, 0, 0);
        jobService.registerJob(runnable);
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L, 1000L);
        job.putAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), "http://example.com");
        final ObjectId id = new ObjectId();
        ReflectionTestUtils.invokeMethod(job, "addProperty", JobInfoProperty.ID, id);
        when(jobInfoRepository.findById(id.toString())).thenReturn(job);
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).thenReturn(job);
        List<String> logLines = Arrays.asList("test", "test1");
        when(remoteJobExecutorService.getStatus(any(URI.class))).thenReturn(
                new RemoteJobStatus(RemoteJobStatus.Status.FINISHED, logLines, new RemoteJobResult(true, 0, "foo"), null));

        jobService.pollRemoteJobs();
        Thread.sleep(1000);
        verify(jobInfoRepository, times(1)).markAsFinished(job.getId(), ResultCode.SUCCESSFUL, "foo");
        assertEquals(ResultCode.SUCCESSFUL, runnable.afterSuccessContext.getResultCode());
    }

    @Test
    public void testPollRemoteJobsJobIsFinishedSuccessfullyAfterExecutionException() throws Exception {
        RemoteMockJobRunnable runnable = new RemoteMockJobRunnable(JOB_NAME_01, remoteJobExecutorService, jobInfoService, 0, 0);
        runnable.throwExceptionInAfterExecution = true;
        jobService.registerJob(runnable);
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L, 1000L);
        job.putAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), "http://example.com");
        final ObjectId id = new ObjectId();
        ReflectionTestUtils.invokeMethod(job, "addProperty", JobInfoProperty.ID, id);
        when(jobInfoRepository.findById(id.toString())).thenReturn(job);
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).thenReturn(job);
        List<String> logLines = Arrays.asList("test", "test1");
        when(remoteJobExecutorService.getStatus(any(URI.class))).thenReturn(
                new RemoteJobStatus(RemoteJobStatus.Status.FINISHED, logLines, new RemoteJobResult(true, 0, "foo"), null));

        jobService.pollRemoteJobs();
        Thread.sleep(100);
        verify(jobInfoRepository, times(1)).markAsFinished(job.getId(), jobExecutionException);
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

    @Test
    public void testListJobRunnables() throws Exception {
        jobService.registerJob(TestSetup.localJobRunnable("job1", 1));
        jobService.registerJob(TestSetup.localJobRunnable("job2", 2));

        final Collection<JobRunnable> jobRunnables = jobService.listJobRunnables();
        assertEquals(2, jobRunnables.size());
        assertEquals("job1", jobRunnables.iterator().next().getJobDefinition().getName());
    }

    private class RemoteMockJobRunnable extends AbstractRemoteJobRunnable {

        public JobExecutionContext afterSuccessContext = null;
        public boolean throwExceptionInAfterExecution = false;
        private AbstractRemoteJobDefinition remoteJobDefinition;

        private RemoteMockJobRunnable(String name, RemoteJobExecutorService rjes, JobInfoService jis, long timeoutPeriod, long pollingInterval) {
            super(rjes, jis);
            remoteJobDefinition = TestSetup.remoteJobDefinition(name, timeoutPeriod, pollingInterval);
        }

        @Override
        public JobDefinition getJobDefinition() {
            return remoteJobDefinition;
        }

        @Override
        public Map<String, String> getParameters() {
            return null;
        }

        @Override
        public void afterExecution(JobExecutionContext context) throws JobException {
            afterSuccessContext = context;
            if (throwExceptionInAfterExecution) {
                jobExecutionException = new JobExecutionException("bar");
                throw jobExecutionException;
            }
        }
    }

    private JobInfo createJobInfo(String name, JobExecutionPriority executionPriority, RunningState runningState) {
        return new JobInfo(name, "test", "test", 1000L, 1000L, runningState, executionPriority, null);
    }

    private StoredJobDefinition createSimpleJd() {
        return new StoredJobDefinition("foo", 0, 0, false, false) ;
    }

}
