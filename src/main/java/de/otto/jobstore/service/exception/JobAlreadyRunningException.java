package de.otto.jobstore.service.exception;


/**
 * Exception which is thrown if a job is already running
 */
public final class JobAlreadyRunningException extends JobException {

    public JobAlreadyRunningException(String s) {
        super(s);
    }

}
