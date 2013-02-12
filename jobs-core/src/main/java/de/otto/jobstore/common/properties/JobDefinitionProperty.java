package de.otto.jobstore.common.properties;

/**
 * Key names used to refer to properties in Job.
 *
 * {@link de.otto.jobstore.common.StoredJobDefinition}
 */
public enum JobDefinitionProperty implements ItemProperty {

    NAME("name"),
    TIMEOUT_PERIOD("timeoutPeriod"),
    POLLING_INTERVAL("pollingInterval"),
    REMOTE("remote"),
    DISABLED("disabled"),
    ABORTABLE("abortable");

    private final String value;

    private JobDefinitionProperty(String value) {
        this.value = value;
    }

    public String val() {
        return value;
    }

}
