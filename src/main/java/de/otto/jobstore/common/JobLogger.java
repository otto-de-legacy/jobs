package de.otto.jobstore.common;

public interface JobLogger {

    void addLoggingData(String log);
    void insertOrUpdateAdditionalData(String key, String value);


}
