package de.otto.jobstore.common.properties;

/**
 * Key names used to refer to properties in JobInfo.
 *
 * {@link de.otto.jobstore.common.JobInfo}
 */
public enum JobInfoProperty implements ItemProperty {

    ID("_id"),
    NAME("name"),
    HOST("host"),
    THREAD("thread"),
    CREATION_TIME("creationTime"),
    START_TIME("startTime"),
    FINISH_TIME("finishTime"),
    PARAMETERS("parameters"),
    EXECUTION_PRIORITY("executionPriority"),
    STATUS_MESSAGE("statusMessage"),
    RESULT_MESSAGE("resultMessage"),
    RUNNING_STATE("runningState"),
    RESULT_STATE("resultState"),
    @Deprecated
    TIMEOUT_PERIOD("maxExecutionTime"),
    MAX_IDLE_TIME("maxIdleTime"),
    MAX_EXECUTION_TIME("maxExecutionTime"),
    LAST_MODIFICATION_TIME("lastModificationTime"),
    ADDITIONAL_DATA("additionalData"),
    LOG_LINES("logLines"),
    REMOTE_JOB_URI("remoteJobUri"),
    ABORTED("aborted");

    private final String value;

    private JobInfoProperty(String value) {
        this.value = value;
    }

    public String val() {
        return value;
    }

}
