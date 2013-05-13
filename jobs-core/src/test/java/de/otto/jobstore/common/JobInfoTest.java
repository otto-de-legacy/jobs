package de.otto.jobstore.common;

import de.otto.jobstore.common.properties.JobInfoProperty;
import org.bson.types.ObjectId;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ReflectionUtils;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Date;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class JobInfoTest {

    @Test
    public void testIsLongTimeIdleReached() throws Exception {
        JobInfo jobInfo = new JobInfo("test", null, null, 60 * 1000L, 60 * 1000L, 0L); //Timeout eine Minute
        Date current = new Date();
        Date lastModified = new Date(current.getTime() - 2 * 60 * 1000L); //Last modified vor 2 Minuten
        jobInfo.setLastModifiedTime(lastModified);
        assertTrue(jobInfo.isIdleTimeExceeded(current)); //Timeout da zuletzt vor zwei Minuten update
        assertFalse(jobInfo.isIdleTimeExceeded(new Date(current.getTime() - Math.round(1.5 * 60 * 1000)))); //Kein Timeout, da vor 500ms update
    }

    @Test
    public void testIsTimeoutReached() throws Exception {
        JobInfo jobInfo = new JobInfo("test", null, null, 1000L, 1000L, 0L); //Timeout eine Sekunde
        ReflectionTestUtils.invokeMethod(jobInfo, "addProperty", JobInfoProperty.START_TIME, jobInfo.getCreationTime());
        Date startTime = jobInfo.getStartTime();

        assertFalse(jobInfo.isTimedOut(new Date(startTime.getTime() + 500))); //Kein Timeout da job erst eine halbe Sekunde alt
        assertTrue(jobInfo.isTimedOut(new Date(startTime.getTime() + 1500))); //Kein Timeout da job erst eine eineinhalb Sekunde alt
    }

}
