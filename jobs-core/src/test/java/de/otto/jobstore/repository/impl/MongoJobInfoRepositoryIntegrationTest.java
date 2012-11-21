package de.otto.jobstore.repository.impl;

import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobInfoProperty;
import org.bson.types.ObjectId;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;

import static org.testng.AssertJUnit.*;

@ContextConfiguration(locations = {"classpath:spring/lhotse-jobs-context.xml"})
public class MongoJobInfoRepositoryIntegrationTest extends AbstractTestNGSpringContextTests {

    private static final String TESTVALUE_JOBNAME = "testjob";
    private static final String TESTVALUE_HOST    = "test";
    private static final String TESTVALUE_THREAD  = "thread";

    @Resource
    private MongoJobInfoRepository jobInfoRepository;

    @BeforeMethod
    public void setup() throws Exception {
        jobInfoRepository.clear(true);
    }

    @Test
    public void testCreate() throws Exception {
        assertFalse(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.RUNNING.name()));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 500, RunningState.RUNNING, false);
        assertTrue(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.RUNNING.name()));
    }

    @Test
    public void testCreatingRunningJobWhichAlreadyExists() throws Exception {
        assertNotNull(jobInfoRepository.create(TESTVALUE_JOBNAME, 60 * 1000, RunningState.RUNNING, false));
        assertNull(jobInfoRepository.create(TESTVALUE_JOBNAME, 60 * 1000, RunningState.RUNNING, false));
    }

    @Test
    public void testQueuedJobNotQueuedAnyMore() throws Exception {
        assertNotNull(jobInfoRepository.create(TESTVALUE_JOBNAME, 60 * 1000, RunningState.QUEUED, false));
        assertTrue(jobInfoRepository.activateQueuedJob(TESTVALUE_JOBNAME));
        assertFalse(jobInfoRepository.activateQueuedJob(TESTVALUE_JOBNAME));

        assertNotNull(jobInfoRepository.create(TESTVALUE_JOBNAME, 60 * 1000, RunningState.QUEUED, false));
        //Would violate Index as running job already exists
        assertFalse(jobInfoRepository.activateQueuedJob(TESTVALUE_JOBNAME));
    }

    @Test
    public void testHasRunningJob() throws InterruptedException {
        assertFalse(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.RUNNING.name()));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        Thread.sleep(200);
        assertTrue(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.RUNNING.name()));
        jobInfoRepository.insertAdditionalData(TESTVALUE_JOBNAME, "key1", "value1");
        Thread.sleep(200);
        assertTrue(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.RUNNING.name()));
    }

    @Test
    public void testHasQueuedJob() throws InterruptedException {
        assertFalse(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.QUEUED.name()));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.QUEUED, false);
        Thread.sleep(200);
        assertTrue(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.QUEUED.name()));
        jobInfoRepository.insertAdditionalData(TESTVALUE_JOBNAME, "key1", "value1");
        Thread.sleep(200);
        assertTrue(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.QUEUED.name()));
    }

    @Test
    public void testClear() {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 300, RunningState.RUNNING, false);
        jobInfoRepository.clear(true);
        assertEquals(0L, jobInfoRepository.count());
    }

    @Test
    public void testMarkAsFinished() throws Exception {
        assertNull(jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING.name()));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 5, RunningState.RUNNING, false);
        assertNull(jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING.name()).getFinishTime());
        jobInfoRepository.markAsFinished(TESTVALUE_JOBNAME, ResultState.SUCCESS, null);
        assertNotNull(jobInfoRepository.findByName(TESTVALUE_JOBNAME).get(0).getFinishTime());
    }

    @Test
    public void testCleanupTimedOutJobs() throws InterruptedException {
        for(int i=0; i < 100; i++) {
            jobInfoRepository.create(TESTVALUE_JOBNAME + i, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        }
        assertEquals(100, jobInfoRepository.count());
        jobInfoRepository.cleanupTimedOutJobs();
        assertEquals(101, jobInfoRepository.count()); //Including cleanup Job
        // mark as finished
        Thread.sleep(1000);
        jobInfoRepository.cleanupTimedOutJobs();
        for(int i=0; i < 100; i++) {
            assertFalse(jobInfoRepository.hasJob(TESTVALUE_JOBNAME + i, RunningState.RUNNING.name()));
        }
    }

    @Test
    public void testAddOrUpdateAdditionalData_Insert() {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        assertEquals(0, jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING.name()).getAdditionalData().size());
        jobInfoRepository.insertAdditionalData(TESTVALUE_JOBNAME, "key1", "value1");
        assertEquals(1, jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING.name()).getAdditionalData().size());
        assertEquals("value1", jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING.name()).getAdditionalData().get("key1"));
    }

    @Test
    public void testAddOrUpdateAdditionalData_Update() {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        jobInfoRepository.insertAdditionalData(TESTVALUE_JOBNAME, "key1", "value1");
        jobInfoRepository.insertAdditionalData(TESTVALUE_JOBNAME, "key1", "value2");
        assertEquals(1, jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING.name()).getAdditionalData().size());
        assertEquals("value2", jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING.name()).getAdditionalData().get("key1"));
    }

    @Test
    public void testAddLogLines() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        jobInfoRepository.addLoggingData(TESTVALUE_JOBNAME, "I am a log!");
        JobInfo runningJob = jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING.name());
        assertFalse(runningJob.getLogLines().isEmpty());
        assertEquals("I am a log!", runningJob.getLogLines().get(0).getLine());
        assertNotNull(runningJob.getLogLines().get(0).getTimestamp());
    }

    @Test
    public void testFindLastBy() {
        jobInfoRepository.create(TESTVALUE_JOBNAME + 1, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        jobInfoRepository.create(TESTVALUE_JOBNAME + 2, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        jobInfoRepository.create(TESTVALUE_JOBNAME + 3, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        assertEquals(3, jobInfoRepository.findLast().size());
        jobInfoRepository.markAsFinished(TESTVALUE_JOBNAME + 1, ResultState.SUCCESS, null);
        jobInfoRepository.create(TESTVALUE_JOBNAME + 1, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        assertEquals(3, jobInfoRepository.findLast().size());
    }

    @Test
    public void testByNameAndTimeRange() {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.QUEUED, false);
        assertEquals(2, jobInfoRepository.findByNameAndTimeRange(TESTVALUE_JOBNAME, new Date(new Date().getTime() - 60 * 1000), null).size());
    }

    @Test
    public void testFindLastByFinishDate() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME + 1, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        assertNull(jobInfoRepository.findLastByNameAndResultState(TESTVALUE_JOBNAME + 1, ResultState.SUCCESS));
        jobInfoRepository.markAsFinished(TESTVALUE_JOBNAME + 1, ResultState.SUCCESS);
        assertNotNull(jobInfoRepository.findLastByNameAndResultState(TESTVALUE_JOBNAME + 1, ResultState.SUCCESS));
    }

    @Test
    public void testAddWithLogLine() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME + "LogLine", TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        JobInfo testJob = jobInfoRepository.findLastByName(TESTVALUE_JOBNAME + "LogLine");
        testJob.appendLogLine(new LogLine("foo", new Date()));
        jobInfoRepository.save(testJob);
        assertEquals(1, testJob.getLogLines().size());
        jobInfoRepository.addLoggingData(TESTVALUE_JOBNAME + "LogLine", "bar");
        testJob = jobInfoRepository.findLastByName(TESTVALUE_JOBNAME + "LogLine");
        assertEquals(2, testJob.getLogLines().size());
    }

    @Test
    public void testFindingLastNotRunning() throws Exception {
        jobInfoRepository.create("test", 1234567890, RunningState.RUNNING, false);
        jobInfoRepository.markAsFinished("test", ResultState.SUCCESS);

        jobInfoRepository.create("test", 1234567890, RunningState.RUNNING, false);
        jobInfoRepository.markAsFinished("test", ResultState.ERROR);

        jobInfoRepository.create("test", 1234567890, RunningState.RUNNING, false);

        List<JobInfo> jobs = jobInfoRepository.findLastNotActive();
        assertFalse(jobs.isEmpty());
        JobInfo job = jobs.get(0);

        assertEquals(ResultState.ERROR, job.getResultState());
    }

    @Test
    public void testFindQueuedJobsSortedAscByCreationTime() throws Exception {
        List<JobInfo> jobs = jobInfoRepository.findQueuedJobsSortedAscByCreationTime();
        assertTrue(jobs.isEmpty());
        jobInfoRepository.create("test", 1000, RunningState.QUEUED, false);
        Thread.sleep(100);
        jobInfoRepository.create("test2", 1000, RunningState.QUEUED, false);
        Thread.sleep(100);
        jobInfoRepository.create("test3", 1000, RunningState.QUEUED, false);
        Thread.sleep(100);
        jobs = jobInfoRepository.findQueuedJobsSortedAscByCreationTime();
        assertEquals("test", jobs.get(0).getName());
        assertEquals("test3", jobs.get(2).getName());
    }

    @Test
    public void testUpdateHostAndThreadInformation() throws Exception {
        assertFalse(jobInfoRepository.updateHostThreadInformation(TESTVALUE_JOBNAME));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        assertTrue(jobInfoRepository.updateHostThreadInformation(TESTVALUE_JOBNAME));
        JobInfo jobInfo = jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING.name());
        assertEquals(Thread.currentThread().getName(), jobInfo.getThread());
        assertEquals(InternetUtils.getHostName(), jobInfo.getHost());
    }

    @Test
    public void testAbortQueuedName() throws Exception {
        assertFalse(jobInfoRepository.abortJob(new ObjectId().toString(), "test"));
        String id = jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        assertTrue(jobInfoRepository.abortJob(id, "test"));
        JobInfo jobInfo = jobInfoRepository.findById(id);
        assertEquals("test", jobInfo.getErrorMessage());
    }

    @Test
    public void testMarkFinishedWithException() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, false);
        jobInfoRepository.markAsFinishedWithException(TESTVALUE_JOBNAME, new IllegalArgumentException("This is an error", new NullPointerException()));
        JobInfo jobInfo = jobInfoRepository.findLastByName(TESTVALUE_JOBNAME);
        assertEquals(ResultState.ERROR, jobInfo.getResultState());
        String runningState = jobInfo.getRunningState();
        assertNotNull(runningState);
    }

    @Test
    public void testFindById() throws Exception {
        String id = jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1234, RunningState.RUNNING, false);
        JobInfo jobInfo = jobInfoRepository.findById(id);
        assertNotNull(jobInfo);
        assertEquals(new Long(1234), jobInfo.getMaxExecutionTime());
    }

    @Test
    public void testFindByInvalidId() throws Exception {
        assertNull(jobInfoRepository.findById("1234"));
    }

    @Test
    public void testCleanupOldRunningJobs() throws Exception {
        JobInfo jobInfo = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.RUNNING);
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.LAST_MODIFICATION_TIME, new Date(new Date().getTime() - 1000 * 60 * 60 * 24 * 5));
        jobInfoRepository.save(jobInfo);
        assertEquals(1L, jobInfoRepository.count());
        jobInfoRepository.cleanupOldJobs(1);
        assertNotNull(jobInfoRepository.findLastByName(TESTVALUE_JOBNAME)); //Job should still be there as it is running
    }

    @Test
    public void testCleanupOldJobs() throws Exception {
        JobInfo jobInfo = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.FINISHED);
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.LAST_MODIFICATION_TIME, new Date(new Date().getTime() - 1000 * 60 * 60 * 24 * 5));
        jobInfoRepository.save(jobInfo);
        assertEquals(1L, jobInfoRepository.count());
        jobInfoRepository.cleanupOldJobs(1);
        assertNull(jobInfoRepository.findLastByName(TESTVALUE_JOBNAME)); //Job should be gone
    }
}
