package de.otto.jobstore.service.impl;

import de.otto.jobstore.common.*;
import de.otto.jobstore.repository.NotFoundException;
import de.otto.jobstore.repository.api.JobInfoRepository;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class JobServiceTest {

    private JobService jobService;
    private JobInfoRepository jobInfoRepository;

    @BeforeMethod
    public void setUp() throws Exception {
        jobInfoRepository = mock(JobInfoRepository.class);
        //
        jobService = new JobService(jobInfoRepository, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRegisterJob_UniqueJobName() throws Exception {
        jobService.registerJob("jobName", createJobInfoCallable());
        jobService.registerJob("jobName", createJobInfoCallable());
    }

    @Test
    public void testStopAllJobs() throws Exception {
        jobService.stopAllJobs();

    }

    @Test(expectedExceptions = NotFoundException.class)
    public void testExecuteQueuedJob_NotFound() throws Exception {
        ReflectionTestUtils.invokeMethod(jobService, "executeQueuedJob", "jobName");
    }

    @Test
    public void testExecuteQueuedJob_JobResultTrue() throws Exception {
        when(jobInfoRepository.hasQueuedJob("jobName")).thenReturn(true);
        when(jobInfoRepository.activateQueuedJob("jobName")).thenReturn(true);
        jobService.registerJob("jobName", createJobInfoCallable());
        // ~
        Boolean executeQueuedJobResult = ReflectionTestUtils.invokeMethod(jobService, "executeQueuedJob", "jobName");
        assertTrue(executeQueuedJobResult);
    }

    @Test
    public void testExecuteQueuedJob_ActivateQueuedJob() throws Exception {
        when(jobInfoRepository.hasQueuedJob("jobName")).thenReturn(true);
        jobService.registerJob("jobName", createJobInfoCallable(Long.MAX_VALUE, true));
        ReflectionTestUtils.invokeMethod(jobService, "executeQueuedJob", "jobName");
        // ~
        verify(jobInfoRepository).activateQueuedJob("jobName");
    }

    @Test
    public void testExecuteQueuedJob_ThrowExceptionIfNonQueued() throws Exception {
        jobService.registerJob("jobName", createJobInfoCallable());
        //
        Boolean executeQueuedJobResult = ReflectionTestUtils.invokeMethod(jobService, "executeQueuedJob", "jobName");
        assertFalse(executeQueuedJobResult);
    }

    @Test
    public void testExecuteQueuedJob_ThrowExceptionIfRunning() throws Exception {
        when(jobInfoRepository.hasRunningJob("jobName")).thenReturn(true);
        jobService.registerJob("jobName", createJobInfoCallable());
        //
        Boolean executeQueuedJobResult = ReflectionTestUtils.invokeMethod(jobService, "executeQueuedJob", "jobName");
        assertFalse(executeQueuedJobResult);
    }

    @Test
    public void testExecuteQueuedJob_Constraint() throws Exception {
        when(jobInfoRepository.hasQueuedJob("jobName")).thenReturn(true);
        when(jobInfoRepository.activateQueuedJob("jobName")).thenReturn(true);
        when(jobInfoRepository.hasRunningJob("jobName2")).thenReturn(true);
        jobService.registerJob("jobName", createJobInfoCallable());
        jobService.registerJob("jobName2", createJobInfoCallable());
        jobService.addRunningConstraint(Arrays.asList("jobName", "jobName2"));
        // ~
        Boolean executeQueuedJobResult = ReflectionTestUtils.invokeMethod(jobService, "executeQueuedJob", "jobName");
        assertFalse(executeQueuedJobResult);
    }

    @Test
    public void testExecuteQueuedJob_MarkAsFinishedWithException() throws Exception {
        when(jobInfoRepository.hasQueuedJob("jobName")).thenReturn(true);
        jobService.registerJob("jobName", new JobRunnable() {
            @Override
            public long getMaxExecutionTime() {
                return 0;
            }

            @Override
            public boolean isExecutionNecessary() {
                return true;
            }

            @Override
            public void execute(JobLogger jobLogger) throws Exception { }
        });
        //
        try {
            ReflectionTestUtils.invokeMethod(jobService, "executeQueuedJob", "jobName");
        } catch(Exception ex) {
            verify(jobInfoRepository).markAsFinishedWithException("jobName", ex);
        }
    }

    @Test
    public void testExecuteQueuedJob_MarkAsFinishedSuccessfully() throws Exception {
        when(jobInfoRepository.hasQueuedJob("jobName")).thenReturn(true);
        when(jobInfoRepository.activateQueuedJob("jobName")).thenReturn(true);
        jobService.registerJob("jobName", createJobInfoCallable());
        // ~
        ReflectionTestUtils.invokeMethod(jobService, "executeQueuedJob", "jobName");
        Thread.sleep(100);
        verify(jobInfoRepository).markAsFinishedSuccessfully("jobName");
    }

    @Test
    public void testStopAllJobs_MarkRunning() throws Exception {
        when(jobInfoRepository.hasRunningJob("jobName")).thenReturn(true);
        when(jobInfoRepository.findRunningByName("jobName")).thenReturn(new JobInfo("jobName", InternetUtils.getHostName(), "bla", 60000L));
        jobService.registerJob("jobName", createJobInfoCallable());
        // ~
        jobService.stopAllJobs();
        verify(jobInfoRepository).markAsFinished("jobName", ResultState.ERROR, "Executing Host was shut down");
    }

    @Test
    public void testStopAllJobs_SkipNoRunning() throws Exception {
        jobService.registerJob("jobName", createJobInfoCallable());
        jobService.registerJob("jobName2", createJobInfoCallable());
        // ~
        jobService.stopAllJobs();
        verify(jobInfoRepository, never()).markAsFinished("jobName2", ResultState.ERROR, "Executing Host was shut down");
    }

    @Test
    public void testExecuteQueuedJobs() throws Exception {
        when(jobInfoRepository.hasQueuedJob("jobName")).thenReturn(true);
            jobService.registerJob("jobName", createJobInfoCallable());
        jobService.registerJob("jobName2", createJobInfoCallable());
        // ~
        jobService.executeQueuedJobs();
        Thread.sleep(100);
        verify(jobInfoRepository, times(1)).activateQueuedJob("jobName");
    }

    @Test
    public void testExecuteQueuedJobs_ASync() throws Exception {
        when(jobInfoRepository.hasQueuedJob("jobName")).thenReturn(true);
        when(jobInfoRepository.hasQueuedJob("jobName2")).thenReturn(true);
        jobService.registerJob("jobName", new JobRunnable() {
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
                Thread.sleep(500);
            }
        });
        jobService.registerJob("jobName2", createJobInfoCallable());
        // ~
        long time = (new Date()).getTime();
        jobService.executeQueuedJobs();
        long diff = ((new Date()).getTime() - time);
        Assert.assertTrue(diff < 450, "diff = " + diff);
        //
        Thread.sleep(600);
        verify(jobInfoRepository, times(2)).activateQueuedJob(anyString());
    }

    @Test(expectedExceptions = NotFoundException.class)
    public void testQueuedJob_NotFound() throws Exception {
        jobService.queueJob("jobName");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testQueueJob_AlreadyQueue() throws Exception {
        jobService.registerJob("jobName", createJobInfoCallable());
        when(jobInfoRepository.hasQueuedJob("jobName")).thenReturn(true);
        // second call throws IllegalStateException
        jobService.queueJob("jobName");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testQueueJob_AlreadyRunning() throws Exception {
        jobService.registerJob("jobName", createJobInfoCallable());
        when(jobInfoRepository.hasRunningJob("jobName")).thenReturn(true);
        // second call throws IllegalStateException
        jobService.queueJob("jobName");
    }

    @Test
    public void testQueueJob() throws Exception {
        when(jobInfoRepository.create("jobName", 0, RunningState.QUEUED, new HashMap<String, String>())).thenReturn("");
        jobService.registerJob("jobName", createJobInfoCallable(0, true));
        jobService.registerJob("jobName2", createJobInfoCallable(0, false));
        Assert.assertNotNull(jobService.queueJob("jobName"));
        Assert.assertNull(jobService.queueJob("jobName2"));
        verify(jobInfoRepository).create("jobName", 0, RunningState.QUEUED, new HashMap<String, String>());
    }

    @Test
    public void testQueueJob_Force() throws Exception {
        HashMap<String, String> additionalData = new HashMap<String, String>();
        additionalData.put("forceExecution", "TRUE");
        when(jobInfoRepository.create("jobName", 0, RunningState.QUEUED, additionalData)).thenReturn("");
        when(jobInfoRepository.create("jobName2", 0, RunningState.QUEUED, additionalData)).thenReturn("");
        jobService.registerJob("jobName", createJobInfoCallable(0, true));
        jobService.registerJob("jobName2", createJobInfoCallable(0, false));
        Assert.assertNotNull(jobService.queueJob("jobName", true));
        Assert.assertNotNull(jobService.queueJob("jobName2", true));
    }

    @Test
    public void testCheckRunningConstraint_WithoutConstraint() throws Exception {
        jobService.registerJob("jobName", createJobInfoCallable());
        jobService.registerJob("jobName2", createJobInfoCallable());
        // ~
        Boolean checkRunningConstraintResult = ReflectionTestUtils.invokeMethod(jobService, "checkRunningConstraint", "jobName");
        assertTrue(checkRunningConstraintResult);
    }

    @Test
    public void testCheckRunningConstraint_WithConstraint() throws Exception {
        jobService.registerJob("jobName", createJobInfoCallable());
        jobService.registerJob("jobName2", createJobInfoCallable());
        jobService.addRunningConstraint(Arrays.asList("jobName", "jobName2"));
        //
        Boolean checkRunningConstraintResult = ReflectionTestUtils.invokeMethod(jobService, "checkRunningConstraint", "jobName");
        assertTrue(checkRunningConstraintResult);
    }

    @Test
    public void testCheckRunningConstraint_WithConstraint_Running() throws Exception {
        jobService.registerJob("jobName", createJobInfoCallable());
        jobService.registerJob("jobName2", createJobInfoCallable());
        when(jobInfoRepository.hasRunningJob("jobName2")).thenReturn(true);
        jobService.addRunningConstraint(Arrays.asList("jobName", "jobName2"));
        //
        Boolean checkRunningConstraintResult = ReflectionTestUtils.invokeMethod(jobService, "checkRunningConstraint", "jobName");
        assertFalse(checkRunningConstraintResult);
    }

    /***
     *
     *  HELPER
     *
     */

    private JobRunnable createJobInfoCallable() {
        return createJobInfoCallable(0, true);
    }

    private JobRunnable createJobInfoCallable(final long maxExecutionTime, final boolean isExecuteNecessary) {
        return new JobRunnable() {

            @Override
            public long getMaxExecutionTime() {
                return maxExecutionTime;
            }

            @Override
            public boolean isExecutionNecessary() {
                return isExecuteNecessary;
            }

            @Override
            public void execute(JobLogger jobLogger) throws Exception { }
        };
    }

}
