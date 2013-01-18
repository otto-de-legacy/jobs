package de.otto.jobstore.common.example;

import de.otto.jobstore.common.AbstractLocalJobDefinition;
import de.otto.jobstore.common.AbstractLocalJobRunnable;
import de.otto.jobstore.common.JobDefinition;
import de.otto.jobstore.common.JobExecutionContext;
import de.otto.jobstore.service.exception.JobExecutionException;

import java.util.Map;

public final class StepTwoJobRunnableExample extends AbstractLocalJobRunnable {

    public static final String STEP_TWO_JOB = "STEP_TWO_JOB";

    @Override
    public JobDefinition getJobDefinition() {
        return new AbstractLocalJobDefinition() {
            @Override
            public String getName() {
                return STEP_TWO_JOB;
            }

            @Override
            public long getTimeoutPeriod() {
                return 1000 * 60 * 20;
            }
        };
    }

    /**
     * Job always finishes with an error
     */
    public void execute(JobExecutionContext executionContext) throws JobExecutionException {
        throw new JobExecutionException("I do not want to work");
    }

}
