package de.otto.jobstore.common.example;

import de.otto.jobstore.common.AbstractLocalJobRunnable;
import de.otto.jobstore.common.JobLogger;
import de.otto.jobstore.common.Parameter;
import de.otto.jobstore.service.JobService;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;

import java.util.Collection;
import java.util.Collections;

public final class StepOneJobRunnableExample extends AbstractLocalJobRunnable {

    private final JobService jobService;

    public StepOneJobRunnableExample(JobService jobService) {
        this.jobService = jobService;
    }

    /**
     * @return name of the simple job, might differ from Classname
     */
    @Override
    public String getName() {
        return "STEP_ONE_JOB";
    }

    @Override
    public Collection<Parameter> getParameters() {
        return Collections.emptyList();
    }

    @Override
    public long getMaxExecutionTime() {
        return 1000 * 60 * 5;
    }

    /**
     * Only execute job one if job two is registered in the job service
     */
    @Override
    public boolean isExecutionNecessary() {
        return jobService.listJobNames().contains(StepTwoJobRunnableExample.STEP_TWO_JOB);
    }

    /**
     * A very lazy job which triggers job two if done
     * @param jobLogger The job logger used to add additional information to a job
     */
    @Override
    public void execute(JobLogger jobLogger) throws JobException {
        try {
            for (int i = 0; i < 10; i++) {
                Thread.sleep(i * 1000);
            }
        } catch (InterruptedException e) {
            throw new JobExecutionException("Interrupted: " + e.getMessage());
        }
        jobService.executeJob(StepTwoJobRunnableExample.STEP_TWO_JOB);
    }
}
