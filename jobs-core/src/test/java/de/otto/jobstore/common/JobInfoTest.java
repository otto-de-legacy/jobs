package de.otto.jobstore.common;

import org.testng.annotations.Test;

import java.util.Date;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class JobInfoTest {

    @Test
    public void testIsTimeoutReached() throws Exception {
        JobInfo jobInfo = new JobInfo("test", null, null, 1000L);
        Date current = new Date();
        Date lastModified = new Date(current.getTime() - 1000);
        jobInfo.setLastModifiedTime(lastModified);
        assertFalse(jobInfo.isTimedOut(new Date(current.getTime() - 500)));
        assertTrue(jobInfo.isTimedOut(new Date(current.getTime() + 500)));
    }

}
