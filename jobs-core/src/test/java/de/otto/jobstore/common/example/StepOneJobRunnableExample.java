package de.otto.jobstore.common.example;

import de.otto.jobstore.common.AbstractLocalJobRunnable;
import de.otto.jobstore.common.JobExecutionContext;
import de.otto.jobstore.common.JobExecutionPriority;
import de.otto.jobstore.common.ResultCode;
import de.otto.jobstore.service.JobService;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;

import java.util.Map;

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
    public Map<String, String> getParameters() {
        return null;
    }

    @Override
    public long getMaxExecutionTime() {
        return 1000 * 60 * 5;
    }

    /**
     * A very lazy job which triggers job two if done
     */
    @Override
    public void execute(JobExecutionContext executionContext) throws JobException {
        if (JobExecutionPriority.CHECK_PRECONDITIONS.equals(executionContext.getExecutionPriority())
                || jobService.listJobNames().contains(StepTwoJobRunnableExample.STEP_TWO_JOB)) {
            executionContext.setResultCode(ResultCode.NOT_EXECUTED);
        }
        try {
            for (int i = 0; i < 10; i++) {
                Thread.sleep(i * 1000);
            }
        } catch (InterruptedException e) {
            throw new JobExecutionException("Interrupted: " + e.getMessage());
        }
        jobService.executeJob(StepTwoJobRunnableExample.STEP_TWO_JOB);
        executionContext.setResultCode(ResultCode.SUCCESSFUL);
    }

}
