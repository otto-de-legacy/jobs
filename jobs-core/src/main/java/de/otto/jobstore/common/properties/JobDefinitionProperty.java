package de.otto.jobstore.common.properties;

/**
 * Key names used to refer to properties in Job.
 *
 * {@link de.otto.jobstore.common.StoredJobDefinition}
 */
public enum JobDefinitionProperty implements ItemProperty {

    NAME("name"),
    @Deprecated
    TIMEOUT_PERIOD("timeoutPeriod"),
    MAX_IDLE_TIME("maxIdleTime"),
    MAX_EXECUTION_TIME("maxExecutionTime"),
    POLLING_INTERVAL("pollingInterval"),
    MAX_RETRIES("maxRetries"),
    RETRY_INTERVAL("retryInterval"),
    REMOTE("remote"),
    DISABLED("disabled", true),
    LAST_NOT_EXECUTED("lastNotExecuted", true),
    ABORTABLE("abortable");

    private final String value;
    private final boolean dynamic;

    private JobDefinitionProperty(String value, boolean dynamic) {
        this.value = value;
        this.dynamic = dynamic;
    }

    private JobDefinitionProperty(String value) {
        this.value = value;
        this.dynamic = false;
    }

    public String val() {
        return value;
    }

    public boolean isDynamic() {
        return dynamic;
    }
}
