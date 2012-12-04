package de.otto.jobstore.common.example;

import de.otto.jobstore.common.JobLogger;
import de.otto.jobstore.common.JobRunnable;
import de.otto.jobstore.service.api.JobService;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;

public final class StepOneJobRunnableExample implements JobRunnable {

    public static final String STEP_ONE_JOB = "STEP_ONE_JOB";
    private final JobService jobService;

    public StepOneJobRunnableExample(JobService jobService) {
        this.jobService = jobService;
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
