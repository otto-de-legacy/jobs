package de.otto.jobstore.common.example;

import de.otto.jobstore.common.JobLogger;
import de.otto.jobstore.common.JobRunnable;
import de.otto.jobstore.service.api.JobService;

public final class StepTwoJobRunnableExample implements JobRunnable {

    public static final String STEP_TWO_JOB = "STEP_TWO_JOB";

    @Override
    public long getMaxExecutionTime() {
        return 1000 * 60 * 20;
    }

    /**
     * No precondition for this job
     */
    @Override
    public boolean isExecutionNecessary() {
        return true;
    }

    /**
     * Job always finishes with an error
     * @param jobLogger The job logger used to add additional information to a job
     * @throws Exception Always thrown
     */
    @Override
    public void execute(JobLogger jobLogger) throws Exception {
        throw new IllegalStateException("I do not want to work");
    }

}
