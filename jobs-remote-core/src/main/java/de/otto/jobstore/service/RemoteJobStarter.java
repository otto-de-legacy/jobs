package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.service.exception.JobException;

import java.net.URI;

public interface RemoteJobStarter {
    URI startJob(RemoteJob job) throws JobException;
}
