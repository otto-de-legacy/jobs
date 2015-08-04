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
import static org.testng.AssertJUnit.fail;
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
    private RemoteMockJobRunnable jobRunnable;

    @BeforeMethod
    public void setUp() throws Exception {
        jobInfoRepository = mock(JobInfoRepository.class);
        jobDefinitionRepository = mock(JobDefinitionRepository.class);
        remoteJobExecutorService = mock(RemoteJobExecutorService.class);
        jobService = new JobService(jobDefinitionRepository, jobInfoRepository);
        jobInfoService = new JobInfoService(jobInfoRepository);
        when(jobDefinitionRepository.find(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName())).thenReturn(StoredJobDefinition.JOB_EXEC_SEMAPHORE);
        jobService.awaitTerminationSeconds = 1;
        jobService.desynchronize = false;
        jobService.startup();
        jobRunnable = new RemoteMockJobRunnable(JOB_NAME_01, remoteJobExecutorService, jobInfoService, 0, 0);
    }

    @Test
    public void queueJobIfConstraintJobIsQueued() throws Exception {
        JobRunnable job1 = TestSetup.localJobRunnable(JOB_NAME_01, 0);
        JobRunnable job2 = TestSetup.localJobRunnable(JOB_NAME_02, 0);

        jobService.registerJob(job1);
        jobService.registerJob(job2);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(new StoredJobDefinition(job1.getJobDefinition()));
        when(jobDefinitionRepository.find(JOB_NAME_02)).thenReturn(new StoredJobDefinition(job2.getJobDefinition()));

        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01);
        constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);

        String jobId2 = "abcd";
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(false);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(true);
        when(jobInfoRepository.create(eq(JOB_NAME_02), anyLong(), anyLong(), anyLong(), eq(RunningState.RUNNING), any(JobExecutionPriority.class), anyMap())).thenReturn(jobId2);
        when(jobInfoRepository.deactivateRunningJob(jobId2)).thenReturn(true);

        jobService.executeJob(JOB_NAME_02);

        verify(jobInfoRepository, times(1)).deactivateRunningJob(jobId2);
    }

    @Test
    public void runQueuedJobEvenIfConstraintJobIsQueued() throws Exception {
        JobRunnable job1 = TestSetup.localJobRunnable(JOB_NAME_01, 0);
        JobRunnable job2 = TestSetup.localJobRunnable(JOB_NAME_02, 0);

        jobService.registerJob(job1);
        jobService.registerJob(job2);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(new StoredJobDefinition(job1.getJobDefinition()));
        when(jobDefinitionRepository.find(JOB_NAME_02)).thenReturn(new StoredJobDefinition(job2.getJobDefinition()));

        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01);
        constraint.add(JOB_NAME_02);
        jobService.addRunningConstraint(constraint);

        String jobId2 = "abcd";
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(true);
        JobInfo jobInfo2 = new JobInfo(JOB_NAME_02, "localhost", "thread", 0L, 0L, 2L, RunningState.QUEUED);
        JobInfo jobInfo2Spy = spy(jobInfo2);
        when(jobInfo2Spy.getId()).thenReturn(jobId2);
        when(jobInfoRepository.activateQueuedJobById(jobId2)).thenReturn(true);

        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(Arrays.asList(jobInfo2Spy));

        jobService.executeQueuedJobs();

        verify(jobInfoRepository).updateHostThreadInformation(jobId2);
    }

    @Test
    public void testRegisteringJob() throws Exception {
        assertTrue(jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0)));
        assertFalse(jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0)));
    }

    @Test
    public void testAddingRunningConstraintForNotExistingJob() throws Exception {
        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01);
        constraint.add(JOB_NAME_02);

        try {
            jobService.addRunningConstraint(constraint);
            fail("expected exception not found");
        } catch(Exception e) {
            assertTrue(e instanceof JobNotRegisteredException);
        }

    }

    @Test
    public void testAddingRunningConstraintWithoutCheckForNotExistingJob() throws Exception {
        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01);
        constraint.add(JOB_NAME_02);
        boolean result = jobService.addRunningConstraintWithoutChecks(constraint);
        assertTrue(result);
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
        System.out.println(Thread.currentThread().getName() + ":" + msg);
    }

    @Test
    public void testMultipleThreadsOnRunningConstraints() throws Throwable {

        final JobRunnable job1 = TestSetup.localJobRunnable(JOB_NAME_01, 0);
        final JobRunnable job2 = TestSetup.localJobRunnable(JOB_NAME_02, 0);
        final String id1 = "id1";
        final String id2 = "id2";

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
                if (countUpdateHostThreadInformation.incrementAndGet() > 1) {
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
        when(jobInfoRepository.activateQueuedJobById(id1)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("activateQueuedJob.1");
                return state1.compareAndSet(0, 1);
            }
        });
        when(jobInfoRepository.deactivateRunningJob(id1)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("deactivateRunningJob.1");
                return state1.compareAndSet(1, 0);
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
        when(jobInfoRepository.activateQueuedJobById(id2)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("activateQueuedJob.2");
                return state2.compareAndSet(0, 1);
            }
        });
        when(jobInfoRepository.deactivateRunningJob(id2)).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                printIt("deactivateRunningJob.2");
                return state2.compareAndSet(1, 0);

            }
        });
        TestFramework.runOnce(new MultithreadedTestCase() {

            public void thread1() throws Exception {
                jobService.executeQueuedJob(job1, id1, JobExecutionPriority.CHECK_PRECONDITIONS);
            }

            public void thread2() throws Exception {
                jobService.executeQueuedJob(job2, id2, JobExecutionPriority.CHECK_PRECONDITIONS);
            }

        });
    }

    @Test
    public void testListingJobNamesRunningConstraintsAndCleaningThem() throws Exception {
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_02, 0));
        Set<String> constraint = new HashSet<>();
        constraint.add(JOB_NAME_01);
        constraint.add(JOB_NAME_02);
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
        JobInfo job = new JobInfo(JOB_NAME_01, InternetUtils.getHostName(), "bla", 60000L, 60000L, 0L);
        ReflectionTestUtils.invokeMethod(job, "addProperty", JobInfoProperty.ID, new ObjectId());
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).thenReturn(job);

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.shutdownJobs();
        verify(jobInfoRepository).markAsFinished(job.getId(), ResultCode.ABORTED, "shutdownJobs called from executing host");
    }

    @Test
    public void testStopAllJobsJobRunningOnDifferentHost() throws Exception {
        JobInfo jobInfo = new JobInfo(JOB_NAME_01, "differentHost", "bla", 60000L, 60000L, 0L);
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
        final ObjectId id1 = new ObjectId();
        JobInfo jobInfo = new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L, 0L);
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.ID, id1);
        when(jobInfoRepository.activateQueuedJobById(id1.toString())).thenReturn(true);
        final ObjectId id2 = new ObjectId();
        final JobInfo jobInfo2 = new JobInfo(JOB_NAME_02, "bla", "bla", 1000L, 1000L, 0L);
        ReflectionTestUtils.invokeMethod(jobInfo2, "addProperty", JobInfoProperty.ID, id2);
        when(jobInfoRepository.activateQueuedJobById(id2.toString())).thenReturn(false);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(jobInfo, jobInfo2));
        TestSetup.LocalMockJobRunnable runnable = TestSetup.localJobRunnable(JOB_NAME_01, 1000);
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
        final ObjectId id = new ObjectId();
        when(jobInfoRepository.activateQueuedJobById(id.toString())).thenReturn(true);
        JobInfo jobInfo = new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L, 0L, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS, Collections.<String, String>emptyMap());
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.ID, id);
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
        final ObjectId id1 = new ObjectId();
        JobInfo jobInfo = new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L, 0L);
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.ID, id1);
        when(jobInfoRepository.activateQueuedJobById(id1.toString())).thenReturn(true);
        final JobInfo jobInfo2 = new JobInfo(JOB_NAME_02, "bla", "bla", 1000L, 1000L, 0L);
        final ObjectId id2 = new ObjectId();
        ReflectionTestUtils.invokeMethod(jobInfo2, "addProperty", JobInfoProperty.ID, id2);
        when(jobInfoRepository.activateQueuedJobById(id2.toString())).thenReturn(false);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(jobInfo, jobInfo2));
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        when(jobDefinitionRepository.find(JOB_NAME_02)).thenReturn(createSimpleJd());
        final JobExecutionException exception = new JobExecutionException("problem while executing");
        JobRunnable runnable = TestSetup.localJobRunnable(JOB_NAME_01, 1000, exception, 0);
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
        final ObjectId id = new ObjectId();
        final JobInfo jobInfo = new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L, 0L);
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.ID, id);
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(jobInfo));
        when(jobInfoRepository.hasJob(JOB_NAME_02, RunningState.RUNNING)).thenReturn(Boolean.TRUE);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        when(jobInfoRepository.activateQueuedJobById(id.toString())).thenReturn(true);

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
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L, 0L)));
        StoredJobDefinition jd = createSimpleJd();
        jd.setDisabled(true);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(jd);

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJobById(anyString());
    }

    @Test
    public void testExecuteQueuedJobAlreadyRunning() throws Exception {
        when(jobInfoRepository.findQueuedJobsSortedAscByCreationTime()).thenReturn(
                Arrays.asList(new JobInfo(JOB_NAME_01, "bla", "bla", 1000L, 1000L, 0L)));
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.TRUE);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        jobService.executeQueuedJobs();

        verify(jobInfoRepository, times(0)).activateQueuedJobById(anyString());
    }

    @Test
    public void testExecuteJobWithSamePriorityOfJobWhichIsAlreadyQueued() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.QUEUED)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.QUEUED));
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        try {
            jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
            jobService.executeJob(JOB_NAME_01);
            fail("expected exception not found");
        } catch(Exception e) {
            assertTrue(e instanceof JobAlreadyQueuedException);
        }
    }

    @Test
    public void testExecuteJobWithHigherPriorityOfJobWhichIsAlreadyQueued() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.QUEUED)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.QUEUED));
        when(jobInfoRepository.create(JOB_NAME_01, 0, 0, 0, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS, JobService.NO_PARAMETERS))
                .thenReturn("1234");
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());

        jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals("1234", id);
    }

    @Test
    public void testExecuteJobWithSamePriorityOfJobWhichIsAlreadyRunning() throws Exception {
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).
                thenReturn(createJobInfo(JOB_NAME_01, JobExecutionPriority.CHECK_PRECONDITIONS, RunningState.RUNNING));
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());


        try {
            jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
            jobService.executeJob(JOB_NAME_01);
            fail("expected exception not found");
        } catch(Exception e) {
            assertTrue(e instanceof JobExecutionNotNecessaryException);
        }

    }

    @Test
    public void testExecuteJobWithHigherPriorityOfJobWhichIsAlreadyRunning() throws Exception {
        when(jobInfoRepository.create(JOB_NAME_01, 0, 0, 0, RunningState.QUEUED, JobExecutionPriority.IGNORE_PRECONDITIONS, JobService.NO_PARAMETERS)).
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
        final String jobId = "1234";
        when(jobInfoRepository.create(JOB_NAME_01, 0, 0, 0, RunningState.RUNNING, JobExecutionPriority.IGNORE_PRECONDITIONS, JobService.NO_PARAMETERS)).
                thenReturn(jobId);
        when(jobInfoRepository.activateQueuedJobById(jobId)).thenReturn(Boolean.TRUE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.FALSE);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        TestSetup.LocalMockJobRunnable runnable = TestSetup.localJobRunnable(JOB_NAME_01, 0);

        jobService.registerJob(runnable);
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals(jobId, id);
        Thread.sleep(500);
        assertTrue(runnable.isExecuted());
    }

    @Test
    public void testExecuteJobForcedFailedWithException() throws Exception {
        final String jobId = "1234";
        when(jobInfoRepository.create(JOB_NAME_01, 0, 0, 0, RunningState.RUNNING, JobExecutionPriority.IGNORE_PRECONDITIONS, JobService.NO_PARAMETERS)).
                thenReturn(jobId);
        when(jobInfoRepository.activateQueuedJobById(JOB_NAME_01)).thenReturn(Boolean.TRUE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.FALSE);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        final JobExecutionException exception = new JobExecutionException("problem while executing");
        JobRunnable runnable = TestSetup.localJobRunnable(JOB_NAME_01, 0, exception, 0);

        jobService.registerJob(runnable);
        String id = jobService.executeJob(JOB_NAME_01, JobExecutionPriority.IGNORE_PRECONDITIONS);
        assertEquals(jobId, id);
        Thread.sleep(500);
        verify(jobInfoRepository, times(1)).markAsFinished(id, exception);
    }

    @Test
    public void testJobExecutedDisabled() throws Exception {
        reset(jobDefinitionRepository);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(createSimpleJd());
        StoredJobDefinition disabledJob = new StoredJobDefinition(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName(), 0, 0, 0, 0, 0, false, false);
        disabledJob.setDisabled(true);
        when(jobDefinitionRepository.find(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName())).thenReturn(disabledJob);
        JobService jobServiceImpl = new JobService(jobDefinitionRepository, jobInfoRepository);

        try {
            jobServiceImpl.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
            jobServiceImpl.executeJob(JOB_NAME_01);
            fail("expected exception not found");
        } catch(Exception e) {
            assertTrue(e instanceof JobExecutionDisabledException);
        }
    }

    @Test
    public void testExecutingJobWhichIsDisabled() throws Exception {
        StoredJobDefinition jd = createSimpleJd();
        jd.setDisabled(true);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(jd);

        try {
            jobService.registerJob(TestSetup.localJobRunnable(JOB_NAME_01, 0));
            jobService.executeJob(JOB_NAME_01);
            fail("expected exception not found");
        } catch(Exception e) {
            assertTrue(e instanceof JobExecutionDisabledException);
        }

    }

    @Test
    public void testRetryJobAsFailed() throws Exception {

        final String jobId = "1234";

        final JobInfo jobInfo = new JobInfo(jobId, "localhost", "thread", 0L, 0L, 2L, RunningState.FINISHED);
        jobInfo.setResultState(ResultCode.FAILED);

        when(jobInfoRepository.findMostRecentFinished(JOB_NAME_01)).thenReturn(jobInfo);
        when(jobInfoRepository.evaluateRetriesBasedOnPreviouslyFailedJobs(JOB_NAME_01, 2L)).thenCallRealMethod();

        when(jobInfoRepository.create(JOB_NAME_01, 0, 0, 2, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, JobService.NO_PARAMETERS)).
                thenReturn(jobId);
        when(jobInfoRepository.activateQueuedJobById(JOB_NAME_01)).thenReturn(Boolean.TRUE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.FALSE);

        StoredJobDefinition jd = new StoredJobDefinition(JOB_NAME_01, 0, 0, 0, 2, 0, false, false);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(jd);

        JobRunnable runnable = TestSetup.localJobRunnable(jd, null);

        jobService.registerJob(runnable);

        jobService.doRetryFailedJobs();

        assertEquals(jobInfoRepository.evaluateRetriesBasedOnPreviouslyFailedJobs(JOB_NAME_01, 2L), 1L);
        verify(jobInfoRepository, times(1)).create(JOB_NAME_01, 0, 0, 2, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, JobService.NO_PARAMETERS);
    }

    @Test
    public void testNoRetryJobAsSuccessFull() throws Exception {

        final String jobId = "1234";

        final JobInfo jobInfo = new JobInfo(jobId, "localhost", "thread", 0L, 0L, 2L, RunningState.FINISHED);
        jobInfo.setResultState(ResultCode.SUCCESSFUL);

        when(jobInfoRepository.findMostRecentFinished(JOB_NAME_01)).thenReturn(jobInfo);
        when(jobInfoRepository.evaluateRetriesBasedOnPreviouslyFailedJobs(JOB_NAME_01, 2L)).thenCallRealMethod();


        when(jobInfoRepository.create(JOB_NAME_01, 0, 0, 2, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, JobService.NO_PARAMETERS)).
                thenReturn(jobId);
        when(jobInfoRepository.activateQueuedJobById(JOB_NAME_01)).thenReturn(Boolean.TRUE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.QUEUED)).thenReturn(Boolean.FALSE);
        when(jobInfoRepository.hasJob(JOB_NAME_01, RunningState.RUNNING)).thenReturn(Boolean.FALSE);

        StoredJobDefinition jd = new StoredJobDefinition(JOB_NAME_01, 0, 0, 0, 2, 0, false, false);
        when(jobDefinitionRepository.find(JOB_NAME_01)).thenReturn(jd);

        JobRunnable runnable = TestSetup.localJobRunnable(jd, null);

        jobService.registerJob(runnable);

        jobService.doRetryFailedJobs();

        assertEquals(jobInfoRepository.evaluateRetriesBasedOnPreviouslyFailedJobs(JOB_NAME_01, 2L), 2L);
        verify(jobInfoRepository, times(0)).create(JOB_NAME_01, 0, 0, 2, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, JobService.NO_PARAMETERS);
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
                thenReturn(new JobInfo(JOB_NAME_01, "host", "thread", 1000L, 1000L, 0L));
        jobService.pollRemoteJobs();
        verify(remoteJobExecutorService, times(0)).getStatus(any(URI.class));
    }

    @Test
    public void testPollRemoteJobsJobStillRunning() throws Exception {
        jobService.registerJob(jobRunnable);
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L, 1000L, 0L);
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

    @Test
    public void testPollRemoteJobsJobIsFinishedNotSuccessfully() throws Exception {
        jobService.registerJob(jobRunnable);
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L, 1000L, 0L);
        job.putAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), "http://example.com");
        ObjectId id = new ObjectId();
        ReflectionTestUtils.invokeMethod(job, "addProperty", JobInfoProperty.ID, id);
        when(jobInfoRepository.findById(id.toString())).thenReturn(job);
        when(jobInfoRepository.findByNameAndRunningState(JOB_NAME_01, RunningState.RUNNING)).thenReturn(job);
        List<String> logLines = Arrays.asList("test", "test1");
        when(remoteJobExecutorService.getStatus(any(URI.class))).thenReturn(
                new RemoteJobStatus(RemoteJobStatus.Status.FINISHED, logLines, new RemoteJobResult(false, 1, "foo"), null));

        jobService.pollRemoteJobs();
        verify(jobInfoRepository, times(1)).markAsFinished(job.getId(), ResultCode.FAILED, "foo");
        // We expect a RemoteJobFailedException, which originates frm execute()
        assertEquals(this.jobRunnable.onExceptionCalled, JobRunnable.State.EXECUTE);
    }

    @Test
    public void testPollRemoteJobsJobIsFinishedSuccessfully() throws Exception {
        RemoteMockJobRunnable runnable = jobRunnable;
        jobService.registerJob(runnable);
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L, 1000L, 0L);
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
        RemoteMockJobRunnable runnable = jobRunnable;
        runnable.throwExceptionInAfterExecution = true;
        jobService.registerJob(runnable);
        JobInfo job = new JobInfo(JOB_NAME_01, "host", "thread", 1000L, 1000L, 0L);
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
        // The exception occurred somewhere in afterExecution()
        assertEquals(jobRunnable.onExceptionCalled, JobRunnable.State.AFTER_EXECUTION);
    }

    @Test
    public void testJobDoesRequireUpdate() throws Exception {
        Date dt = new Date();
        Date lastModification = new Date(dt.getTime() - 60L * 1000L);
        long currentTime = dt.getTime();
        long pollingInterval = 30 * 1000L;
        boolean requiresUpdate = ReflectionTestUtils.invokeMethod(jobService, "jobAgedOverInterval", lastModification, currentTime, pollingInterval);
        assertTrue(requiresUpdate);
    }

    @Test
    public void testJobDoesNotRequireUpdate() throws Exception {
        Date dt = new Date();
        Date lastModification = new Date(dt.getTime() - 60L * 1000L);
        long currentTime = dt.getTime();
        long pollingInterval = 90 * 1000L;
        boolean requiresUpdate = ReflectionTestUtils.invokeMethod(jobService, "jobAgedOverInterval", lastModification, currentTime, pollingInterval);
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

    @Test
    public void executesTimedOutJobsCleanup() throws Exception {
        jobService.cleanupTimedOutJobs();

        verify(jobInfoRepository).cleanupTimedOutJobs();
    }

    private class RemoteMockJobRunnable extends AbstractRemoteJobRunnable {

        public JobExecutionContext afterSuccessContext = null;
        public boolean throwExceptionInAfterExecution = false;
        private AbstractRemoteJobDefinition remoteJobDefinition;
        private State onExceptionCalled = null;

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
        public void doAfterExecution(JobExecutionContext context) throws JobException {
            afterSuccessContext = context;
            if (throwExceptionInAfterExecution) {
                jobExecutionException = new JobExecutionException("bar");
                throw jobExecutionException;
            }
        }

        @Override
        public OnException onException(JobExecutionContext context, Exception e, State state) {
            this.onExceptionCalled = state;
            return super.onException(context, e, state);
        }
    }

    private JobInfo createJobInfo(String name, JobExecutionPriority executionPriority, RunningState runningState) {
        return new JobInfo(name, "test", "test", 1000L, 1000L, 0L, runningState, executionPriority, Collections.<String, String>emptyMap());
    }

    private StoredJobDefinition createSimpleJd() {
        return new StoredJobDefinition("foo", 0, 0, 0, 0, 0, false, false);
    }

}
