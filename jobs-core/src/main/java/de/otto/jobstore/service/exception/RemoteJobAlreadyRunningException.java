package de.otto.jobstore.service.exception;


import java.net.URI;

/**
 * Exception which is thrown if a job is already running
 */
public final class RemoteJobAlreadyRunningException extends JobException {

    private final URI jobUri;

    public RemoteJobAlreadyRunningException(String s, URI jobUri) {
        super(s);
        this.jobUri = jobUri;
    }

    public URI getJobUri() {
        return jobUri;
    }

}
