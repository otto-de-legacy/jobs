package de.otto.jobstore.service.exception;


/**
 * Exception which is thrown if a Job is already Queued
 */
public final class JobAlreadyQueuedException extends JobException {

    public JobAlreadyQueuedException(String s) {
        super(s);
    }

}
