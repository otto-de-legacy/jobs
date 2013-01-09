package de.otto.jobstore.common;

import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.service.RemoteJobExecutorService;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public abstract class AbstractRemoteJobRunnable implements JobRunnable {

    private final RemoteJobExecutorService remoteJobExecutorService;
    protected Logger log = LoggerFactory.getLogger(this.getClass());

    protected AbstractRemoteJobRunnable(RemoteJobExecutorService remoteJobExecutorService) {
        this.remoteJobExecutorService = remoteJobExecutorService;
    }

    @Override
    public final boolean isRemote() {
        return true;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobException {
        context.setRunningState(RunningState.RUNNING);
    }

    @Override
    public void beforeExecution(JobExecutionContext context) throws JobException {
        final JobLogger jobLogger = context.getJobLogger();
        try {
            log.info("Trigger remote job '{}' [{}] ...", getName(), context.getId());
            final URI uri = remoteJobExecutorService.startJob(new RemoteJob(getName(), context.getId(), getParameters()));
            jobLogger.insertOrUpdateAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), uri.toString());
        } catch (RemoteJobAlreadyRunningException e) {
            log.info("Remote job '{}' [{}] is already running: " + e.getMessage(), getName(), context.getId());
            jobLogger.insertOrUpdateAdditionalData("resumedAlreadyRunningJob", e.getJobUri().toString());
            jobLogger.insertOrUpdateAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), e.getJobUri().toString());
        }
    }

    @Override
    public void afterExecution(JobExecutionContext context) throws JobException {
    }

}
