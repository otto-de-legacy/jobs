package de.otto.jobstore.common;


import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertTrue;

public class JobExecutionPriorityTest {

    @Test
    public void testLowerJobPriority() throws Exception {
        assertTrue(JobExecutionPriority.CHECK_PRECONDITIONS.isLowerThan(JobExecutionPriority.IGNORE_PRECONDITIONS));
        assertTrue(JobExecutionPriority.IGNORE_PRECONDITIONS.isLowerThan(JobExecutionPriority.FORCE_EXECUTION));
    }

    @Test
    public void testHigherOrEqualsPriority() throws Exception {
        assertTrue(JobExecutionPriority.CHECK_PRECONDITIONS.isEqualOrHigherThan(JobExecutionPriority.CHECK_PRECONDITIONS));
        assertTrue(JobExecutionPriority.IGNORE_PRECONDITIONS.isEqualOrHigherThan(JobExecutionPriority.CHECK_PRECONDITIONS));
        assertTrue(JobExecutionPriority.FORCE_EXECUTION.isEqualOrHigherThan(JobExecutionPriority.IGNORE_PRECONDITIONS));
    }

}
