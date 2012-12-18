package de.otto.jobstore.common;

import org.testng.annotations.Test;

import java.util.Date;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class JobInfoTest {

    @Test
    public void testIsTimeoutReached() throws Exception {
        JobInfo jobInfo = new JobInfo("test", null, null, 60 * 1000L); //Timeout eine Minute
        Date current = new Date();
        Date lastModified = new Date(current.getTime() - 2 * 60 * 1000L); //Last modified vor 2 Minuten
        jobInfo.setLastModifiedTime(lastModified);
        assertTrue(jobInfo.isTimedOut(current)); //Timeout da zuletzt vor zwei Minuten update
        assertFalse(jobInfo.isTimedOut(new Date(current.getTime() - Math.round(1.5 * 60 * 1000)))); //Kein Timeout, da vor 500ms update
    }

}
