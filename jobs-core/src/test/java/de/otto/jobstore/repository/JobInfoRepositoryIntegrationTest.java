package de.otto.jobstore.repository;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobInfoProperty;
import org.bson.types.ObjectId;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Resource;
import java.util.*;

import static org.testng.AssertJUnit.*;

@ContextConfiguration(locations = {"classpath:spring/lhotse-jobs-context.xml"})
public class JobInfoRepositoryIntegrationTest extends AbstractTestNGSpringContextTests {

    private static final String TESTVALUE_JOBNAME = "testjob";
    private static final String TESTVALUE_HOST    = "test";
    private static final String TESTVALUE_THREAD  = "thread";

    @Resource
    private JobInfoRepository jobInfoRepository;

    @BeforeMethod
    public void setup() throws Exception {
        jobInfoRepository.clear(true);
    }

    @Test
    public void testCreate() throws Exception {
        assertFalse(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.RUNNING));
        Map<String, String> params = new HashMap<String, String>();
        params.put("foo", "bar");
        params.put("hugo", "moep");
        String id = jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 500, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, params, null);
        assertTrue(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.RUNNING));
        JobInfo createdJob = jobInfoRepository.findById(id);
        assertNotNull("Created job with id " + id + ", cannot be found", createdJob);
        assertNotNull("Created job has no parameters set", createdJob.getParameters());
        assertEquals(2, createdJob.getParameters().size());
    }

    @Test
    public void testCreatingRunningJobWhichAlreadyExists() throws Exception {
        assertNotNull(jobInfoRepository.create(TESTVALUE_JOBNAME, 60 * 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null));
        assertNull(jobInfoRepository.create(TESTVALUE_JOBNAME, 60 * 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null));
    }

    @Test
    public void testQueuedJobNotQueuedAnyMore() throws Exception {
        assertNotNull(jobInfoRepository.create(TESTVALUE_JOBNAME, 60 * 1000, RunningState.QUEUED, JobExecutionPriority.CHECK_PRECONDITIONS, null, null));
        assertTrue(jobInfoRepository.activateQueuedJob(TESTVALUE_JOBNAME));
        assertFalse(jobInfoRepository.activateQueuedJob(TESTVALUE_JOBNAME));

        assertNotNull(jobInfoRepository.create(TESTVALUE_JOBNAME, 60 * 1000, RunningState.QUEUED, JobExecutionPriority.CHECK_PRECONDITIONS, null, null));
        //Would violate Index as running job already exists
        assertFalse(jobInfoRepository.activateQueuedJob(TESTVALUE_JOBNAME));
    }

    @Test
    public void testHasRunningJob() throws InterruptedException {
        assertFalse(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.RUNNING));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        Thread.sleep(200);
        assertTrue(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.RUNNING));
        jobInfoRepository.addAdditionalData(TESTVALUE_JOBNAME, "key1", "value1");
        Thread.sleep(200);
        assertTrue(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.RUNNING));
    }

    @Test
    public void testHasQueuedJob() throws InterruptedException {
        assertFalse(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.QUEUED));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.QUEUED, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        Thread.sleep(200);
        assertTrue(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.QUEUED));
        jobInfoRepository.addAdditionalData(TESTVALUE_JOBNAME, "key1", "value1");
        Thread.sleep(200);
        assertTrue(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.QUEUED));
    }

    @Test
    public void testClear() {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 300, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        jobInfoRepository.clear(true);
        assertEquals(0L, jobInfoRepository.count());
    }

    @Test
    public void testMarkAsFinished() throws Exception {
        assertNull(jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING));
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 5, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        assertNull(jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING).getFinishTime());
        jobInfoRepository.markRunningAsFinished(TESTVALUE_JOBNAME, ResultCode.SUCCESSFUL, null);
        assertNotNull(jobInfoRepository.findByName(TESTVALUE_JOBNAME, null).get(0).getFinishTime());
    }

    @Test
    public void testMarkAsFinishedById() throws Exception {
        assertNull(jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING));
        String id = jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 5, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        assertNull(jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING).getFinishTime());
        jobInfoRepository.markAsFinishedById(id, ResultCode.SUCCESSFUL, null);
        assertNotNull(jobInfoRepository.findByName(TESTVALUE_JOBNAME, null).get(0).getFinishTime());
    }

    @Test
    public void testCleanupTimedOutJobs() throws InterruptedException {
        for(int i=0; i < 100; i++) {
            jobInfoRepository.create(TESTVALUE_JOBNAME + i, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        }
        assertEquals(100, jobInfoRepository.count());
        jobInfoRepository.cleanupTimedOutJobs();
        assertEquals(101, jobInfoRepository.count()); //Including cleanup Job
        // mark as finished
        Thread.sleep(1000);
        jobInfoRepository.cleanupTimedOutJobs();
        for(int i=0; i < 100; i++) {
            assertFalse(jobInfoRepository.hasJob(TESTVALUE_JOBNAME + i, RunningState.RUNNING));
        }
    }

    @Test
    public void testAddOrUpdateAdditionalData_Insert() {
        String id = jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        assertEquals(0, jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING).getAdditionalData().size());
        jobInfoRepository.addAdditionalData(TESTVALUE_JOBNAME, "key1", "value1");
        JobInfo jobInfo = jobInfoRepository.findById(id);
        assertEquals(1, jobInfo.getAdditionalData().size());
        assertEquals("value1", jobInfo.getAdditionalData().get("key1"));
    }

    @Test
    public void testAddOrUpdateAdditionalData_Update() {
        String id =
                jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        jobInfoRepository.addAdditionalData(TESTVALUE_JOBNAME, "key1", "value1");
        jobInfoRepository.addAdditionalData(TESTVALUE_JOBNAME, "key1", "value2");
        JobInfo jobInfo = jobInfoRepository.findById(id);
        assertEquals(1, jobInfo.getAdditionalData().size());
        assertEquals("value2", jobInfo.getAdditionalData().get("key1"));
    }

    @Test
    public void testAddLogLines() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        jobInfoRepository.addLogLine(TESTVALUE_JOBNAME, "I am a log!");
        JobInfo runningJob = jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING);
        assertFalse(runningJob.getLogLines().isEmpty());
        assertEquals("I am a log!", runningJob.getLogLines().get(0).getLine());
        assertNotNull(runningJob.getLogLines().get(0).getTimestamp());
    }

    @Test
    public void testFindLastBy() {
        jobInfoRepository.create(TESTVALUE_JOBNAME + 1, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        jobInfoRepository.create(TESTVALUE_JOBNAME + 2, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        jobInfoRepository.create(TESTVALUE_JOBNAME + 3, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        assertEquals(3, jobInfoRepository.findMostRecent().size());
        jobInfoRepository.markRunningAsFinished(TESTVALUE_JOBNAME + 1, ResultCode.SUCCESSFUL, null);
        jobInfoRepository.create(TESTVALUE_JOBNAME + 1, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        assertEquals(3, jobInfoRepository.findMostRecent().size());
    }

    @Test
    public void testByNameAndTimeRange() {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.QUEUED, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        assertEquals(2, jobInfoRepository.findByNameAndTimeRange(TESTVALUE_JOBNAME, new Date(new Date().getTime() - 60 * 1000), null).size());
    }

    @Test
    public void testFindLastByFinishDate() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME + 1, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        assertNull(jobInfoRepository.findMostRecentByNameAndResultState(TESTVALUE_JOBNAME + 1, EnumSet.of(ResultCode.SUCCESSFUL)));
        jobInfoRepository.markRunningAsFinished(TESTVALUE_JOBNAME + 1, ResultCode.SUCCESSFUL, null);
        assertNotNull(jobInfoRepository.findMostRecentByNameAndResultState(TESTVALUE_JOBNAME + 1, EnumSet.of(ResultCode.SUCCESSFUL)));
    }

    @Test
    public void testAddWithLogLine() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME + "LogLine", TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        JobInfo testJob = jobInfoRepository.findMostRecentByName(TESTVALUE_JOBNAME + "LogLine");
        testJob.appendLogLine(new LogLine("foo", new Date()));
        jobInfoRepository.save(testJob);
        assertEquals(1, testJob.getLogLines().size());
        jobInfoRepository.addLogLine(TESTVALUE_JOBNAME + "LogLine", "bar");
        testJob = jobInfoRepository.findMostRecentByName(TESTVALUE_JOBNAME + "LogLine");
        assertEquals(2, testJob.getLogLines().size());
    }

    @Test
    public void testFindQueuedJobsSortedAscByCreationTime() throws Exception {
        List<JobInfo> jobs = jobInfoRepository.findQueuedJobsSortedAscByCreationTime();
        assertTrue(jobs.isEmpty());
        jobInfoRepository.create("test", 1000, RunningState.QUEUED, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        Thread.sleep(100);
        jobInfoRepository.create("test2", 1000, RunningState.QUEUED, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        Thread.sleep(100);
        jobInfoRepository.create("test3", 1000, RunningState.QUEUED, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        Thread.sleep(100);
        jobs = jobInfoRepository.findQueuedJobsSortedAscByCreationTime();
        assertEquals("test", jobs.get(0).getName());
        assertEquals("test3", jobs.get(2).getName());
    }

    @Test
    public void testUpdateHostAndThreadInformation() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        jobInfoRepository.updateHostThreadInformation(TESTVALUE_JOBNAME);
        JobInfo jobInfo = jobInfoRepository.findByNameAndRunningState(TESTVALUE_JOBNAME, RunningState.RUNNING);
        assertEquals(Thread.currentThread().getName(), jobInfo.getThread());
        assertEquals(InternetUtils.getHostName(), jobInfo.getHost());
    }

    @Test
    public void testMarkFinishedWithException() throws Exception {
        jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
        jobInfoRepository.markRunningAsFinishedWithException(TESTVALUE_JOBNAME, new IllegalArgumentException("This is an error", new NullPointerException()));
        JobInfo jobInfo = jobInfoRepository.findMostRecentByName(TESTVALUE_JOBNAME);
        assertEquals(ResultCode.FAILED, jobInfo.getResultState());
        String runningState = jobInfo.getRunningState();
        assertNotNull(runningState);
    }

    @Test
    public void testFindById() throws Exception {
        String id = jobInfoRepository.create(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1234, RunningState.RUNNING, JobExecutionPriority.CHECK_PRECONDITIONS, null, null);
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
        jobInfoRepository.setDaysAfterWhichOldJobsAreDeleted(1);
        JobInfo jobInfo = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.RUNNING);
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.LAST_MODIFICATION_TIME, new Date(new Date().getTime() - 1000 * 60 * 60 * 24 * 5));
        jobInfoRepository.save(jobInfo);
        assertEquals(1L, jobInfoRepository.count());
        jobInfoRepository.cleanupOldJobs();
        assertNotNull(jobInfoRepository.findMostRecentByName(TESTVALUE_JOBNAME)); //Job should still be there as it is running
    }

    @Test
    public void testCleanupOldJobs() throws Exception {
        jobInfoRepository.setDaysAfterWhichOldJobsAreDeleted(1);
        JobInfo jobInfo = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.FINISHED);
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.CREATION_TIME, new Date(new Date().getTime() - 1000 * 60 * 60 * 24 * 12));
        jobInfoRepository.save(jobInfo);
        assertEquals(1L, jobInfoRepository.count());
        jobInfoRepository.cleanupOldJobs();
        assertNull(jobInfoRepository.findMostRecentByName(TESTVALUE_JOBNAME)); //Job should be gone
    }

    @Test
    public void testFindMostRecentByResultState() throws Exception {
        JobInfo jobInfo = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.RUNNING);
        jobInfoRepository.save(jobInfo);
        jobInfoRepository.markRunningAsFinished(TESTVALUE_JOBNAME, ResultCode.FAILED, null);
        JobInfo jobInfo1 = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.RUNNING);
        jobInfoRepository.save(jobInfo1);
        jobInfoRepository.markRunningAsFinished(TESTVALUE_JOBNAME, ResultCode.SUCCESSFUL, null);
        JobInfo jobInfo2 = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.RUNNING);
        jobInfoRepository.save(jobInfo2);
        jobInfoRepository.markRunningAsFinished(TESTVALUE_JOBNAME, ResultCode.TIMED_OUT, null);
        JobInfo jobInfo3 = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.RUNNING);
        jobInfoRepository.save(jobInfo3);
        jobInfoRepository.markRunningAsFinished(TESTVALUE_JOBNAME, ResultCode.NOT_EXECUTED, null);

        JobInfo notExecuted = jobInfoRepository.findMostRecentByNameAndResultState(TESTVALUE_JOBNAME,
                EnumSet.of(ResultCode.NOT_EXECUTED));
        assertEquals(ResultCode.NOT_EXECUTED, notExecuted.getResultState());

        JobInfo timedOut = jobInfoRepository.findMostRecentByNameAndResultState(TESTVALUE_JOBNAME,
                EnumSet.complementOf(EnumSet.of(ResultCode.NOT_EXECUTED)));
        assertEquals(ResultCode.TIMED_OUT, timedOut.getResultState());
    }

    @Test
    public void testFindMostRecentByResultStateOnlyNotExecuted() throws Exception {
        JobInfo jobInfo = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.RUNNING);
        jobInfoRepository.save(jobInfo);
        jobInfoRepository.markRunningAsFinished(TESTVALUE_JOBNAME, ResultCode.NOT_EXECUTED, null);

        JobInfo notExecuted = jobInfoRepository.findMostRecentByNameAndResultState(TESTVALUE_JOBNAME,
                EnumSet.of(ResultCode.NOT_EXECUTED));
        assertEquals(ResultCode.NOT_EXECUTED, notExecuted.getResultState());

        JobInfo job = jobInfoRepository.findMostRecentByNameAndResultState(TESTVALUE_JOBNAME,
                EnumSet.complementOf(EnumSet.of(ResultCode.NOT_EXECUTED)));
        assertNull(job);
    }

    @Test
    public void testSetQueuedJobAsNotExecuted() throws Exception {
        assertFalse(jobInfoRepository.markQueuedAsNotExecuted("test"));
        JobInfo jobInfo = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.QUEUED);
        jobInfoRepository.save(jobInfo);
        jobInfoRepository.markQueuedAsNotExecuted(TESTVALUE_JOBNAME);
        List<JobInfo> jobInfoList = jobInfoRepository.findByName(TESTVALUE_JOBNAME, null);
        assertEquals(1, jobInfoList.size());
        assertEquals(ResultCode.NOT_EXECUTED, jobInfoList.get(0).getResultState());
    }

    @Test
    public void testRemoveJobIfTimedOut() throws Exception {
        JobInfo jobInfo = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.RUNNING);
        jobInfo.setLastModifiedTime(new Date(new Date().getTime() - 60 * 60 * 1000));
        jobInfoRepository.save(jobInfo);
        assertEquals(1, jobInfoRepository.count());
        jobInfoRepository.removeJobIfTimedOut(TESTVALUE_JOBNAME, new Date());
        assertFalse(jobInfoRepository.hasJob(TESTVALUE_JOBNAME, RunningState.RUNNING));
    }

    @Test
    public void testSetLogLines() throws Exception {
        JobInfo jobInfo = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.RUNNING);
        jobInfoRepository.save(jobInfo);
        jobInfoRepository.setLogLines(TESTVALUE_JOBNAME, Arrays.asList("test1", "test2", "test3"));
        JobInfo retrievedJobInfo = jobInfoRepository.findMostRecentByName(TESTVALUE_JOBNAME);
        assertEquals(3, retrievedJobInfo.getLogLines().size());
    }

    @Test
    public void testCleanupTimedOutJob() throws Exception {
        DBObject queuedJob = new BasicDBObject()
                .append("_id", new ObjectId("50c99099e4b048a05ee9a024"))
                .append("creationTime", new Date())
                .append("forceExecution", false)
                .append("lastModificationTime", new Date())
                .append("maxExecutionTime", 300000L)
                .append("name", "ProductRelationFeedImportJob")
                .append("runningState", "QUEUED")
                .append("thread", "productSystemScheduler-3");
        DBObject runningJob = new BasicDBObject()
                .append("_id", new ObjectId("60c99099e4b048a05ee9a024"))
                .append("creationTime", new Date(new GregorianCalendar(2012, 11, 13, 8, 23, 53).getTimeInMillis()))
                .append("forceExecution", false)
                .append("lastModificationTime", new Date(new GregorianCalendar(2012, 11, 13, 8, 59, 0).getTimeInMillis()))
                .append("maxExecutionTime", 300000L)
                .append("name", "ProductRelationFeedImportJob")
                .append("runningState", "RUNNING")
                .append("startTime", new Date(new GregorianCalendar(2012, 11, 13, 8, 23, 53).getTimeInMillis()))
                .append("thread", "productSystemScheduler-3");
        jobInfoRepository.save(new JobInfo(queuedJob));
        jobInfoRepository.save(new JobInfo(runningJob));
        assertEquals(2, jobInfoRepository.count());
        assertEquals(1, jobInfoRepository.cleanupTimedOutJobs());
        JobInfo timedOutJob = jobInfoRepository.findById("60c99099e4b048a05ee9a024");
        assertEquals(ResultCode.TIMED_OUT, timedOutJob.getResultState());
    }

    @Test
    public void testRemoveJob() throws Exception {
        JobInfo jobInfo = new JobInfo(TESTVALUE_JOBNAME, TESTVALUE_HOST, TESTVALUE_THREAD, 1000L, RunningState.RUNNING);
        jobInfoRepository.save(jobInfo);
        assertNotNull(jobInfoRepository.findById(jobInfo.getId()));
        jobInfoRepository.remove(jobInfo.getId());
        assertNull(jobInfoRepository.findById(jobInfo.getId()));
    }
}
