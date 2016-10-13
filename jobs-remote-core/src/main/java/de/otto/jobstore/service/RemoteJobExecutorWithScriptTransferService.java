package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.common.RemoteJobStatus;
import de.otto.jobstore.service.exception.JobException;

import java.net.URI;

/**
 * This class triggers the execution of jobs on a remote server.
 * The scripts for execution are sent within the request body.
 *
 * The remote server has to expose a rest endpoint.
 * The multipart request sent to the endpoint consists of two parts:
 *
 * 1) A binary part containing the tar file:
 *     Content-Disposition: form-data; name="scripts"; filename="scripts.tar.gz"
 *     Content-Type: application/octet-stream
 *     Content-Transfer-Encoding: binary
 * 2) A part containing the JSON formatted parameters:
 *     Content-Disposition: form-data; name="params"
 *     Content-Type: application/json; charset=UTF-8
 *     Content-Transfer-Encoding: 8bit
 */
public class RemoteJobExecutorWithScriptTransferService implements RemoteJobExecutor {
    private final RemoteJobExecutor delegate;
    private final RemoteJobStarter remoteJobStarter;

    RemoteJobExecutorWithScriptTransferService(RemoteJobExecutor delegate, RemoteJobStarter remoteJobStarter) {
        this.delegate = delegate;
        this.remoteJobStarter = remoteJobStarter;
    }

    public static RemoteJobExecutorWithScriptTransferService create(RemoteJobExecutor delegate, String jobExecutorUri, TarArchiveProvider tarArchiveProvider) {
        return create(delegate, new HttpClientBasedRemoteJobStarter(jobExecutorUri, tarArchiveProvider));
    }

    public static RemoteJobExecutorWithScriptTransferService create(RemoteJobExecutor delegate, RemoteJobStarter remoteJobStarter) {
        return new RemoteJobExecutorWithScriptTransferService(delegate, remoteJobStarter);
    }

    @Override
    public String getJobExecutorUri() {
        return delegate.getJobExecutorUri();
    }

    @Override
    public URI startJob(RemoteJob job) throws JobException {
        return remoteJobStarter.startJob(job);
    }

    @Override
    public void stopJob(URI jobUri) throws JobException {
        delegate.stopJob(jobUri);

    }

    @Override
    public RemoteJobStatus getStatus(URI jobUri) {
        return delegate.getStatus(jobUri);
    }

    @Override
    public boolean isAlive() {
        return delegate.isAlive();
    }
}
