package de.otto.jobstore.common.properties;


/**
 * Properties of JobInfo
 *
 * {@link de.otto.jobstore.common.JobInfo}
 */
public enum JobInfoProperty implements ItemProperty {

    ID("_id"),
    NAME("name"),
    HOST("host"),
    THREAD("thread"),
    START_TIME("startTime"),
    FINISH_TIME("finishTime"),
    ERROR_MESSAGE("errorMessage"),
    RUNNING_STATE("runningState"),
    RESULT_STATE("resultState"),
    TIMEOUT("timeout"),
    LAST_MODIFICATION_TIME("lastModificationTime"),
    ADDITIONAL_DATA("additionalData"),
    LOG_LINES("logLines");

    private final String value;

    private JobInfoProperty(String value) {
        this.value = value;
    }

    public String val() {
        return value;
    }

}
