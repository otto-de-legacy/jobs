package de.otto.jobstore.service.exception;


/**
 * Exception which is thrown if a job was aborted
 */
public final class JobExecutionAbortedException extends JobException {

    public JobExecutionAbortedException(String s) {
        super(s);
    }

    public static JobExecutionAbortedException fromJobName(String name) {
        return new JobExecutionAbortedException("Job with name " + name + " was aborted");
    }

}
