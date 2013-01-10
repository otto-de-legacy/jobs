package de.otto.jobstore.common;

import de.otto.jobstore.service.exception.JobException;

public abstract class AbstractLocalJobRunnable implements JobRunnable {

    @Override
    public final long getPollingInterval() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean isRemote() {
        return false;
    }

    /**
     * By default returns true, override for your custom needs,
     */
    @Override
    public boolean checkPreconditions(JobExecutionContext context) {
        return true;
    }

    @Override
    public void beforeExecution(JobExecutionContext context) throws JobException {}

    @Override
    public void execute(JobExecutionContext context) throws JobException {
        if (checkPreconditions(context)) {
            context.setRunningState(RunningState.RUNNING);
            doExecute(context);
        } else {
            context.setResultCode(ResultCode.NOT_EXECUTED);
            context.setRunningState(RunningState.FINISHED);
        }
    }

    /**
     * In most cases it might be good enough to simply implement this method.
     */
    protected abstract void doExecute(JobExecutionContext context) throws JobException;


    @Override
    public void afterExecution(JobExecutionContext context) throws JobException {}

}
