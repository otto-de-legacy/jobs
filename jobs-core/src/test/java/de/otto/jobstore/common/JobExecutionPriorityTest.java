package de.otto.jobstore.common;


import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertTrue;

public class JobExecutionPriorityTest {

    @Test
    public void testLowerJobPriority() throws Exception {
        assertTrue(JobExecutionPriority.CHECK_PRECONDITIONS.hasLowerPriority(JobExecutionPriority.IGNORE_PRECONDITIONS));
        assertTrue(JobExecutionPriority.IGNORE_PRECONDITIONS.hasLowerPriority(JobExecutionPriority.FORCE_EXECUTION));
    }

}
