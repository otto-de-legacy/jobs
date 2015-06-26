package de.otto.jobstore.common;

import de.otto.jobstore.service.exception.JobException;

import java.util.Collections;
import java.util.Map;

public abstract class AbstractLocalJobRunnable implements JobRunnable {

    @Override
    public final RemoteJobStatus getRemoteStatus(JobExecutionContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getParameters() {
        return Collections.emptyMap();
    }

    /**
     * By default returns true. If an exception occurs, returns false.
     */
    @Override
    public boolean prepare(JobExecutionContext context) throws JobException {
        try {
            return doPrepare(context);
        } catch (Exception e) {
            return onException(context, e, State.PREPARE).hasRecovered();
        }
    }

    @Override
    public void execute(JobExecutionContext context) throws JobException {
        try {
            doExecute(context);
        } catch (Exception e) {
            onException(context, e, State.EXECUTE).doThrow();
        }
    }

    /**
     * Template method for de.otto.jobstore.common.AbstractLocalJobRunnable#execute(de.otto.jobstore.common.JobExecutionContext)
     */
    protected void doExecute(JobExecutionContext context) throws JobException {
    }

    /**
     * Template method of de.otto.jobstore.common.AbstractLocalJobRunnable#prepare(de.otto.jobstore.common.JobExecutionContext)
     */
    protected boolean doPrepare(JobExecutionContext context) throws JobException {
        return true;
    }

    /**
     * Implementation might want to set the {@link JobExecutionContext#resultCode}
     */
    @Override
    public void afterExecution(JobExecutionContext context) throws JobException {
        try {
            doAfterExecution(context);
        } catch (Exception e) {
            onException(context, e, State.AFTER_EXECUTION).doThrow();
        }
    }

    /**
     * Template method for de.otto.jobstore.common.AbstractLocalJobRunnable#afterExecution(de.otto.jobstore.common.JobExecutionContext)
     */
    protected void doAfterExecution(JobExecutionContext context) throws JobException {
    }

    @Override
    public OnException onException(JobExecutionContext context, Exception e, State state) {
        return new DefaultOnException(e);
    }
}
