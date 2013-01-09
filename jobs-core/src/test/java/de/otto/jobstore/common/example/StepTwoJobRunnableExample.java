package de.otto.jobstore.common.example;

import de.otto.jobstore.common.AbstractLocalJobRunnable;
import de.otto.jobstore.common.JobLogger;
import de.otto.jobstore.service.exception.JobExecutionException;

import java.util.Map;

public final class StepTwoJobRunnableExample extends AbstractLocalJobRunnable {

    public static final String STEP_TWO_JOB = "STEP_TWO_JOB";

    /**
     * @return name of the simple job, might differ from Classname
     */
    @Override
    public String getName() {
        return STEP_TWO_JOB;
    }

    @Override
    public Map<String, String> getParameters() {
        return null;
    }

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
     */
    @Override
    public void execute(JobLogger jobLogger) throws JobExecutionException {
        throw new JobExecutionException("I do not want to work");
    }

}
