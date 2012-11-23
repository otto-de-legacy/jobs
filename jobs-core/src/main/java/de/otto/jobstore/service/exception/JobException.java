package de.otto.jobstore.service.exception;


public abstract class JobException extends RuntimeException {

    public JobException(String s) {
        super(s);
    }

    protected JobException(String s, Throwable throwable) {
        super(s, throwable);
    }

}
