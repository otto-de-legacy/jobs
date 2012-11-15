package de.otto.jobstore.common;

/**
 * Interface for
 */
public interface JobLogger {

    /**
     *
     * @param log
     */
    void addLoggingData(String log);

    /**
     *
     * @param key
     * @param value
     */
    void insertOrUpdateAdditionalData(String key, String value);

}
