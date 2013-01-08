package de.otto.jobstore.common.example;

import de.otto.jobstore.common.AbstractLocalJobRunnable;
import de.otto.jobstore.common.JobExecutionResult;
import de.otto.jobstore.common.JobExecutionContext;
import de.otto.jobstore.service.exception.JobExecutionException;

public final class StepTwoJobRunnableExample extends AbstractLocalJobRunnable {

    public static final String STEP_TWO_JOB = "STEP_TWO_JOB";

    /**
     * @return name of the simple job, might differ from Classname
     */
    public String getName() {
        return STEP_TWO_JOB;
    }

    @Override
    public long getMaxExecutionTime() {
        return 1000 * 60 * 20;
    }

    /**
     * Job always finishes with an error
     * @param executionContext The context in which this job is executed
     */
    @Override
    public JobExecutionResult execute(JobExecutionContext executionContext) throws JobExecutionException {
        throw new JobExecutionException("I do not want to work");
    }

}
