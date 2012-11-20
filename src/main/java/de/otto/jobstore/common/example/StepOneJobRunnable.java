package de.otto.jobstore.common.example;

import de.otto.jobstore.common.JobLogger;
import de.otto.jobstore.common.JobRunnable;
import de.otto.jobstore.service.api.JobService;

public final class StepOneJobRunnable implements JobRunnable {

    public static final String STEP_ONE_JOB = "STEP_ONE_JOB";
    private final JobService jobService;

    public StepOneJobRunnable(JobService jobService) {
        this.jobService = jobService;
    }

    @Override
    public long getMaxExecutionTime() {
        return 1000 * 60 * 5;
    }

    /**
     * Only execute job one if job two is registered in the job service
     * @return
     */
    @Override
    public boolean isExecutionNecessary() {
        return jobService.listJobNames().contains(StepTwoJobRunnable.STEP_TWO_JOB);
    }

    /**
     * A very lazy job which triggers job two if done
     * @param jobLogger The job logger used to add additional information to a job
     */
    @Override
    public void execute(JobLogger jobLogger) throws Exception {
        for (int i = 0; i < 10; i++) {
            Thread.sleep(i * 1000);
        }
        jobService.queueJob(StepTwoJobRunnable.STEP_TWO_JOB);
    }
}
