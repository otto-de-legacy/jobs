package de.otto.jobstore.service.exception;


/**
 * Exception which is thrown if a Job is not registered
 */
public final class JobNotRegisteredException extends JobException {

    public JobNotRegisteredException(String s) {
        super(s);
    }

}
