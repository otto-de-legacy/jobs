package de.otto.jobstore.service.exception;


import java.net.URI;

/**
 * Exception which is thrown if a job is already running
 */
public final class RemoteJobNotRunningException extends JobException {

    public RemoteJobNotRunningException(String s, Throwable t) {
        super(s, t);
    }

    public RemoteJobNotRunningException(String s) {
        super(s);
    }
}
