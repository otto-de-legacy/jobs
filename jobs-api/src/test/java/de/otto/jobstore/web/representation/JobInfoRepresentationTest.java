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
        JobInfoRepresentation jobInfoRep = JobInfoRepresentation.fromJobInfo(jobInfo);

        assertEquals("foo", jobInfoRep.getName());
        assertEquals("host", jobInfoRep.getHost());
        assertEquals("thread", jobInfoRep.getThread());
        assertEquals(2, jobInfoRep.getLogLines().size());
        assertEquals(2, jobInfoRep.getAdditionalData().size());
    }

}
