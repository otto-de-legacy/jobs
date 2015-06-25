package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.common.RemoteJobStatus;
import de.otto.jobstore.service.exception.JobException;

import java.net.URI;

public interface RemoteJobExecutor {
    String getJobExecutorUri();

    URI startJob(RemoteJob job) throws JobException;

    void stopJob(URI jobUri) throws JobException;

    RemoteJobStatus getStatus(URI jobUri);

    boolean isAlive();
}
