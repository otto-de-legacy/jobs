package de.otto.jobstore.common.properties;


/**
 * Properties of LogLIne
 *
 * {@link de.otto.jobstore.common.LogLine}
 */
public enum LogLineProperty implements ItemProperty {

    LINE("line"),
    TIMESTAMP("timestamp");

    private final String value;

    private LogLineProperty(String value) {
        this.value = value;
    }

    public String val() {
        return value;
    }

}
