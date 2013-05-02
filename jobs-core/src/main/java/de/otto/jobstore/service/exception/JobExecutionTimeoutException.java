package de.otto.jobstore.service.exception;


/**
 * Exception which is thrown if a job was aborted
 */
public final class JobExecutionTimeoutException extends JobException {

    public JobExecutionTimeoutException(String s) {
        super(s);
    }

    public static JobExecutionTimeoutException fromJobName(String name) {
        return new JobExecutionTimeoutException("Job with name " + name + " reached timeout");
    }

}
