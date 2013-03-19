package de.otto.jobstore.service;

import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class JobServiceRunningConstraintsIntegrationTest {


    private JobService jobService1;

    private JobService jobService2;

    @BeforeMethod
    public void setUp() throws Exception {
        jobService1 = mock(JobService.class);
        jobService2 = mock(JobService.class);


    }

    @Test
    public void testRunningConstraints() throws Throwable {

        TestFramework.runOnce(new MultithreadedTestCase() {

            public void thread1() {
                assertTick(1);
            }

            public void thread2() {
                waitForTick(1);

            }

        });

    }

}