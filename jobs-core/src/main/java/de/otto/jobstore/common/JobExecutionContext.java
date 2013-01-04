package de.otto.jobstore.common;

public final class JobExecutionContext {

    private final JobLogger jobLogger;
    private final JobExecutionPriority executionPriority;

    public JobExecutionContext(JobLogger jobLogger, JobExecutionPriority executionPriority) {
        this.jobLogger = jobLogger;
        this.executionPriority = executionPriority;
    }

    public JobLogger getJobLogger() {
        return jobLogger;
    }

    public JobExecutionPriority getExecutionPriority() {
        return executionPriority;
    }

}
