package de.otto.jobstore.repository.impl;

import com.mongodb.Mongo;
import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.LogLine;
import de.otto.jobstore.common.ResultState;
import de.otto.jobstore.common.RunningState;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;

import static org.testng.AssertJUnit.*;

public class MongoJobInfoRepositoryIntegrationTest {

    private static final String TESTVALUE_JOBNAME = "testjob";
    private static final String TESTVALUE_HOST    = "test";
    private static final String TESTVALUE_THREAD  = "thread";

    private MongoJobInfoRepository jobInfoRepository;

    @BeforeClass
    public void init() throws Exception {
        Mongo mongo = new Mongo("127.0.0.1");
        jobInfoRepository = new MongoJobInfoRepository(mongo, "lhotse-jobs", "jobs_test",
                new MongoIdRepository(mongo, "lhotse-jobs", "ids_test"));
    }

    @BeforeMethod
    public void setup() throws Exception {
        jobInfoRepository.clear(true);
    }

    @Test
    public void testCreate() throws Exception {
        assertFalse(jobInfoRepository.hasRunningJob(TESTVALUE_JOBNAME));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 500, RunningState.RUNNING);
        assertTrue(jobInfoRepository.hasRunningJob(TESTVALUE_JOBNAME));
    }

    @Test
    public void testCreatingRunningJobWhichAlreadyExists() throws Exception {
        assertNotNull(jobInfoRepository.create(TESTVALUE_JOBNAME, 60 * 1000));
        assertNull(jobInfoRepository.create(TESTVALUE_JOBNAME, 60 * 1000));
    }

    @Test
    public void testQueuedJobNotQueuedAnyMore() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME, 60 * 1000, RunningState.QUEUED);
        assertTrue(jobInfoRepository.activateQueuedJob(TESTVALUE_JOBNAME));
        assertFalse(jobInfoRepository.activateQueuedJob(TESTVALUE_JOBNAME));
    }

    @Test
    public void testHasRunningJob() throws InterruptedException {
        assertFalse(jobInfoRepository.hasRunningJob(TESTVALUE_JOBNAME));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING);
        Thread.sleep(200);
        assertTrue(jobInfoRepository.hasRunningJob(TESTVALUE_JOBNAME));
        jobInfoRepository.insertAdditionalData(TESTVALUE_JOBNAME, "key1", "value1");
        Thread.sleep(200);
        assertTrue(jobInfoRepository.hasRunningJob(TESTVALUE_JOBNAME));
    }

    @Test
    public void testClear() {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 300, RunningState.RUNNING);
        jobInfoRepository.clear(true);
        assertEquals(0L, jobInfoRepository.count());
    }

    @Test(enabled = false) //TODO: causes to much trouble on jenkins execution
    public void testCreateTTL() throws Exception {
        assertFalse(jobInfoRepository.hasRunningJob(TESTVALUE_JOBNAME));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 500, RunningState.RUNNING);
        assertTrue(jobInfoRepository.hasRunningJob(TESTVALUE_JOBNAME));
        Thread.sleep(100);
        jobInfoRepository.cleanupTimedOutJobs();
        assertTrue(jobInfoRepository.hasRunningJob(TESTVALUE_JOBNAME));
        Thread.sleep(1500);
        jobInfoRepository.cleanupTimedOutJobs();
        assertFalse(jobInfoRepository.hasRunningJob(TESTVALUE_JOBNAME));
    }

    @Test
    public void testMarkAsFinished() throws Exception {
        assertNull(jobInfoRepository.findRunningByName(TESTVALUE_JOBNAME));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 5, RunningState.RUNNING);
        assertNull(jobInfoRepository.findRunningByName(TESTVALUE_JOBNAME).getFinishTime());
        jobInfoRepository.markAsFinished(TESTVALUE_JOBNAME, ResultState.SUCCESS, null);
        assertNotNull(jobInfoRepository.findByName(TESTVALUE_JOBNAME).get(0).getFinishTime());
    }

    @Test(enabled = false)
    public void testCleanup_byIsFinished() {
        for(int i=0; i < 100; i++) {
            jobInfoRepository.create(TESTVALUE_JOBNAME + i, TESTVALUE_HOST, TESTVALUE_THREAD, 50000, RunningState.RUNNING);
        }
        assertEquals(100, jobInfoRepository.count());
        jobInfoRepository.clear(false);
        assertEquals(100, jobInfoRepository.count());
        // mark as finished
        for(int i=0; i < 100; i++) {
            jobInfoRepository.markAsFinished(TESTVALUE_JOBNAME + i, ResultState.SUCCESS, null);
        }
        jobInfoRepository.clear(false);
        assertEquals(0, jobInfoRepository.count());
    }

    @Test(enabled = false)
    public void testCleanup_byMaxExecutionTime() throws InterruptedException {
        for(int i=0; i < 100; i++) {
            jobInfoRepository.create(TESTVALUE_JOBNAME + i, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING);
        }
        assertEquals(100, jobInfoRepository.count());
        jobInfoRepository.clear(false);
        assertEquals(100, jobInfoRepository.count());
        // mark as finished
        Thread.sleep(1000);
        jobInfoRepository.cleanupTimedOutJobs();
        for(int i=0; i < 100; i++) {
            assertFalse(jobInfoRepository.hasRunningJob(TESTVALUE_JOBNAME + i));
        }
    }

    @Test
    public void testAddOrUpdateAdditionalData_Insert() {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING);
        assertEquals(0, jobInfoRepository.findRunningByName(TESTVALUE_JOBNAME).getAdditionalData().size());
        jobInfoRepository.insertAdditionalData(TESTVALUE_JOBNAME, "key1", "value1");
        assertEquals(1, jobInfoRepository.findRunningByName(TESTVALUE_JOBNAME).getAdditionalData().size());
        assertEquals("value1", jobInfoRepository.findRunningByName(TESTVALUE_JOBNAME).getAdditionalData().get("key1"));
    }

    @Test
    public void testAddOrUpdateAdditionalData_Update() {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING);
        jobInfoRepository.insertAdditionalData(TESTVALUE_JOBNAME, "key1", "value1");
        jobInfoRepository.insertAdditionalData(TESTVALUE_JOBNAME, "key1", "value2");
        assertEquals(1, jobInfoRepository.findRunningByName(TESTVALUE_JOBNAME).getAdditionalData().size());
        assertEquals("value2", jobInfoRepository.findRunningByName(TESTVALUE_JOBNAME).getAdditionalData().get("key1"));
    }

    @Test
    public void testFindLastBy() {
        jobInfoRepository.create(TESTVALUE_JOBNAME + 1, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING);
        jobInfoRepository.create(TESTVALUE_JOBNAME + 2, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING);
        jobInfoRepository.create(TESTVALUE_JOBNAME + 3, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING);
        assertEquals(3, jobInfoRepository.findLast().size());
        jobInfoRepository.markAsFinished(TESTVALUE_JOBNAME + 1, ResultState.SUCCESS, null);
        jobInfoRepository.create(TESTVALUE_JOBNAME + 1, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING);
        assertEquals(3, jobInfoRepository.findLast().size());
    }

    @Test(enabled = false)
    public void testByNameAndTimerange() {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING);
        //jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, "FINISHED_42");
        //jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, "FINISHED_73");
        assertEquals(3, jobInfoRepository.findByNameAndTimeRange(TESTVALUE_JOBNAME, new Date(new Date().getTime() - 60 * 1000), null).size());
    }

    @Test
    public void testFindLastByFinishDate() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME + 1, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING);
        assertNull(jobInfoRepository.findLastByNameAndResultState(TESTVALUE_JOBNAME + 1, ResultState.SUCCESS));
        jobInfoRepository.markAsFinished(TESTVALUE_JOBNAME + 1, ResultState.SUCCESS);
        assertNotNull(jobInfoRepository.findLastByNameAndResultState(TESTVALUE_JOBNAME + 1, ResultState.SUCCESS));
    }

    @Test
    public void testAddWithLogline() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME + "LogLine", TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING);
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
        jobInfoRepository.create("test", 1234567890);
        jobInfoRepository.markAsFinished("test", ResultState.SUCCESS);

        jobInfoRepository.create("test", 1234567890);
        jobInfoRepository.markAsFinished("test", ResultState.ERROR);

        jobInfoRepository.create("test", 1234567890);

        List<JobInfo> jobs = jobInfoRepository.findLastNotActive();
        assertFalse(jobs.isEmpty());
        JobInfo job = jobs.get(0);

        assertEquals(ResultState.ERROR, job.getResultState());
    }



}
