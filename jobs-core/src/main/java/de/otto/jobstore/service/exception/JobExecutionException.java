package de.otto.jobstore.service.exception;

/**
 * Exception for the case something goes bad while a job is executed.
 */
public final class JobExecutionException extends JobException {

    public JobExecutionException(String s) {
        super(s);
    }

}
