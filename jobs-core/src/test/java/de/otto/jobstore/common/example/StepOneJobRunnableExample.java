package de.otto.jobstore.common.example;

import de.otto.jobstore.common.*;
import de.otto.jobstore.service.JobService;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;

public final class StepOneJobRunnableExample extends AbstractLocalJobRunnable {

    private final JobService jobService;

    /**
     * @return name of the simple job, might differ from Classname
     */
    public String getName() {
        return "STEP_ONE_JOB";
    }

    public StepOneJobRunnableExample(JobService jobService) {
        this.jobService = jobService;
    }

    @Override
    public long getMaxExecutionTime() {
        return 1000 * 60 * 5;
    }

    /**
     * A very lazy job which triggers job two if done
     * @param executionContext The context in which this job is executed
     */
    @Override
    public JobExecutionResult execute(JobExecutionContext executionContext) throws JobException {
        if (JobExecutionPriority.CHECK_PRECONDITIONS.equals(executionContext.getExecutionPriority())
                || jobService.listJobNames().contains(StepTwoJobRunnableExample.STEP_TWO_JOB)) {
            return new JobExecutionResult(RunningState.FINISHED, ResultCode.NOT_EXECUTED);
        }
        try {
            for (int i = 0; i < 10; i++) {
                Thread.sleep(i * 1000);
            }
        } catch (InterruptedException e) {
            throw new JobExecutionException("Interrupted: " + e.getMessage());
        }
        jobService.executeJob(StepTwoJobRunnableExample.STEP_TWO_JOB);
        return new JobExecutionResult(RunningState.FINISHED, ResultCode.SUCCESSFUL);
    }
}
