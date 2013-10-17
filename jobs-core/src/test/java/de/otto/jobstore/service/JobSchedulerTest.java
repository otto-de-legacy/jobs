package de.otto.jobstore.service;

import de.otto.jobstore.common.JobSchedule;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.AssertJUnit.assertTrue;

public class JobSchedulerTest {


    @BeforeMethod
    public void setUp() throws Exception {
    }

    @Test
    public void testSchedulingOfJobs() throws Exception {
        JobSchedule jobSchedule1 = JobSchedule.create("jobSchedule1",100, new Runnable() {
            @Override
            public void run() {
                throw new RuntimeException();
            }
        });
        JobSchedule jobSchedule2 = JobSchedule.create("jobSchedule2",200, null);

        JobScheduler jobScheduler = new JobScheduler(Arrays.asList(jobSchedule1, jobSchedule2));
        jobScheduler.startup();
        Thread.sleep(1200);
        jobScheduler.shutdown();

        assertTrue(jobSchedule1.count() > 10);
        assertTrue(jobSchedule2.count() > 5);
    }

}
