package de.otto.jobstore.service.exception;


/**
 * Exception which is thrown if execution of a job is not necessary
 */
public final class JobExecutionNotNecessaryException extends JobException {

    public JobExecutionNotNecessaryException(String s) {
        super(s);
    }

}
