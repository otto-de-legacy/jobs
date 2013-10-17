package de.otto.jobstore.service.exception;


/**
 * Exception which is thrown if the job service is not active
 */
public final class JobServiceNotActiveException extends JobException {

    public JobServiceNotActiveException(String s) {
        super(s);
    }

}
