package de.otto.jobstore.service.api;

import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.common.RemoteJobStatus;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;

import java.net.URI;


public interface RemoteJobExecutorService {

    URI startJob(RemoteJob job) throws JobException;

    void stopJob(URI jobUri) throws JobException;

    RemoteJobStatus getStatus(URI jobUri);

}
