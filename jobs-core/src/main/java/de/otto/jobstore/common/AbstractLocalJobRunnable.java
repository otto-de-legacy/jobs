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
     * By default returns true.
     * If an exception occurs, calls de.otto.jobstore.common.JobRunnable#onException(de.otto.jobstore.common.JobExecutionContext, java.lang.Exception, de.otto.jobstore.common.JobRunnable.State)
     * and rethrows the exception.
     */
    @Override
    public boolean prepare(JobExecutionContext context) throws JobException {
        try {
            return doPrepare(context);
        } catch (Exception e) {
            onException(context, e, State.PREPARE);
            return true;
        }
    }

    /**
     * Only triggers the remote job, poll to check wether job is finished or not.
     * If an exception occurs, calls de.otto.jobstore.common.JobRunnable#onException(de.otto.jobstore.common.JobExecutionContext, java.lang.Exception, de.otto.jobstore.common.JobRunnable.State)
     * and rethrows the exception.
     *
     * @see de.otto.jobstore.service.JobService#pollRemoteJobs()
     */
    @Override
    public void execute(JobExecutionContext context) throws JobException {
        try {
            doExecute(context);
        } catch (Exception e) {
            onException(context, e, State.EXECUTE);
        }
    }

    /**
     * Template method for de.otto.jobstore.common.AbstractLocalJobRunnable#execute(de.otto.jobstore.common.JobExecutionContext)
     */
    protected void doExecute(JobExecutionContext context) throws JobException {
    }

    /**
     * Template method of de.otto.jobstore.common.AbstractLocalJobRunnable#prepare(de.otto.jobstore.common.JobExecutionContext)
     * By default returns true, override for your custom needs.
     */
    protected boolean doPrepare(JobExecutionContext context) throws JobException {
        return true;
    }

    /**
     * Implementation might want to set the {@link JobExecutionContext#resultCode}
     * If an exception occurs, calls de.otto.jobstore.common.JobRunnable#onException(de.otto.jobstore.common.JobExecutionContext, java.lang.Exception, de.otto.jobstore.common.JobRunnable.State)
     * and rethrows the exception.
     */
    @Override
    public void afterExecution(JobExecutionContext context) throws JobException {
        try {
            doAfterExecution(context);
        } catch (Exception e) {
            onException(context, e, State.AFTER_EXECUTION);
        }
    }

    /**
     * Template method for de.otto.jobstore.common.AbstractLocalJobRunnable#afterExecution(de.otto.jobstore.common.JobExecutionContext)
     */
    protected void doAfterExecution(JobExecutionContext context) throws JobException {
    }

    /**
     * Extension point for side effects after an exception occurred.
     * By default rethrows exception. If it does not rethrow the exception, the exception has been handled already,
     * and no exception is thrown.
     */
    @Override
    public void onException(JobExecutionContext context, Exception e, State state) throws JobException {
        if (e instanceof JobException) {
            throw (JobException)e;
        } else {
            throw new JobException("Unexpected exception during job.", e) {};
        }
    }
}
