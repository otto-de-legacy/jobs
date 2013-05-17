package de.otto.jobstore.service;

import de.otto.jobstore.TestSetup;
import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.common.util.InternetUtils;
import de.otto.jobstore.repository.JobDefinitionRepository;
import de.otto.jobstore.repository.JobInfoRepository;
import de.otto.jobstore.service.exception.*;
import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;
import org.bson.types.ObjectId;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
