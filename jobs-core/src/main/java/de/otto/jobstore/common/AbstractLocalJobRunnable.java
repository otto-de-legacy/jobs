package de.otto.jobstore.common;

import de.otto.jobstore.service.exception.JobException;

public abstract class AbstractLocalJobRunnable implements JobRunnable {

    @Override
    public final long getPollingInterval() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RemoteJobStatus getRemoteStatus(JobExecutionContext context) {
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
    public boolean prepare(JobExecutionContext context) {
        return true;
    }

    @Override
    public void afterExecution(JobExecutionContext context) throws JobException {}

}
