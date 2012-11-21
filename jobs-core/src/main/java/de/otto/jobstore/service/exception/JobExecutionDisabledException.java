package de.otto.jobstore.service.exception;


/**
 * Exception which is thrown if a job is already running
 */
public final class JobExecutionDisabledException extends JobException {

    public JobExecutionDisabledException(String s) {
        super(s);
    }

}
