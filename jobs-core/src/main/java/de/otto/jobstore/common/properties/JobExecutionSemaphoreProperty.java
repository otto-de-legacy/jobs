package de.otto.jobstore.common.properties;

public enum JobExecutionSemaphoreProperty implements ItemProperty {

    NAME("name"),
    DISABLED("disabled");

    private final String value;

    private JobExecutionSemaphoreProperty(String value) {
        this.value = value;
    }

    public String val() {
        return value;
    }

}
