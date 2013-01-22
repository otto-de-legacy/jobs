package de.otto.jobstore.web.representation;

import de.otto.jobstore.common.JobExecutionPriority;
import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.LogLine;
import de.otto.jobstore.common.RunningState;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.HashMap;

import static org.testng.AssertJUnit.assertEquals;

public class JobInfoRepresentationTest {

    @Test
    public void testFromJobInfo() throws Exception {
        JobInfo jobInfo = new JobInfo("foo", "host", "thread", 1234L, RunningState.RUNNING, JobExecutionPriority.IGNORE_PRECONDITIONS, new HashMap<String, String>());
        jobInfo.appendLogLine(new LogLine("line1", new Date()));
        jobInfo.appendLogLine(new LogLine("line2", new Date()));
        jobInfo.putAdditionalData("key1", "value1");
        jobInfo.putAdditionalData("key2", "value1");
        JobInfoRepresentation jobInfoRep = JobInfoRepresentation.fromJobInfo(jobInfo, 100);

        assertEquals("foo", jobInfoRep.getName());
        assertEquals("host", jobInfoRep.getHost());
        assertEquals("thread", jobInfoRep.getThread());
        assertEquals(2, jobInfoRep.getLogLines().size());
        assertEquals(2, jobInfoRep.getAdditionalData().size());
    }

    @Test
    public void testCutoffLogLines() throws Exception {
        JobInfo jobInfo = new JobInfo("foo", "host", "thread", 1234L, RunningState.RUNNING, JobExecutionPriority.IGNORE_PRECONDITIONS, new HashMap<String, String>());
        for (int i = 0; i < 50; i++) {
            jobInfo.appendLogLine(new LogLine("line " + i, new Date()));
        }
        jobInfo.putAdditionalData("key1", "value1");
        jobInfo.putAdditionalData("key2", "value1");
        JobInfoRepresentation jobInfoRep = JobInfoRepresentation.fromJobInfo(jobInfo, 20);
        assertEquals(20, jobInfoRep.getLogLines().size());
        assertEquals("line 30", jobInfoRep.getLogLines().get(0).getLine());
        assertEquals("line 49", jobInfoRep.getLogLines().get(19).getLine());
    }

}
