package de.otto.jobstore.common;

/**
 * Adds logging and/or additional data to a job being executed
 */
public interface JobLogger {

    /**
     * Adds logging data to the job
     *
     * @param log The log line
     */
    void addLoggingData(String log);

    /**
     * Adds additional data to the job
     *
     * @param key The key of the additional data
     * @param value The data to be added
     */
    void insertOrUpdateAdditionalData(String key, String value);

}
