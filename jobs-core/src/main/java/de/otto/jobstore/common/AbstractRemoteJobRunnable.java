package de.otto.jobstore.common;


import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.service.api.RemoteJobExecutorService;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;

import java.net.URI;
import java.util.List;

public abstract class AbstractRemoteJobRunnable implements JobRunnable {

    private final RemoteJobExecutorService remoteJobExecutorService;

    protected AbstractRemoteJobRunnable(RemoteJobExecutorService remoteJobExecutorService) {
        this.remoteJobExecutorService = remoteJobExecutorService;
    }

    @Override
    public final boolean isRemote() {
        return true;
    }

    protected abstract List<Parameter> getParameters();

    @Override
    public void execute(JobLogger jobLogger) throws JobException {
        try {
            final URI uri = remoteJobExecutorService.startJob(new RemoteJob(getName(), getParameters()));
            jobLogger.insertOrUpdateAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), uri.toString());
        } catch (RemoteJobAlreadyRunningException e) {
            jobLogger.insertOrUpdateAdditionalData("resumedAlreadyRunningJob", e.getJobUri().toString());
            jobLogger.insertOrUpdateAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), e.getJobUri().toString());
        }
    }

}
