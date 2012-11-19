package de.otto.jobstore.common;

import org.testng.annotations.Test;

import java.util.Date;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class JobInfoTest {

    @Test
    public void testIsTimeoutReached() throws Exception {
        JobInfo jobInfo = new JobInfo("test", null, null, 1000);
        Date current = new Date();
        Date lastModified = new Date(current.getTime() - 1000);
        jobInfo.setLastModifiedTime(lastModified);
        assertFalse(jobInfo.isTimedOut(new Date(current.getTime() - 500)));
        assertTrue(jobInfo.isTimedOut(new Date(current.getTime() + 500)));
    }

    @Test
    public void testGetLogLinesString() throws Exception {
        JobInfo jobInfo = new JobInfo("test", null, null, 1000);
        jobInfo.appendLogLine(new LogLine("test", new Date(0)));
        jobInfo.appendLogLine(new LogLine("test2", new Date(60 * 60 * 24 * 1000)));
        assertEquals("01/01/1970-01:00:00 CET: test\n" +
                "02/01/1970-01:00:00 CET: test2", jobInfo.getLogLineString());
    }
}
