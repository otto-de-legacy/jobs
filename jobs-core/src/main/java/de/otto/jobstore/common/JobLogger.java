package de.otto.jobstore.common;

import java.util.List;

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

    List<String> getLoggingData();

    /**
    * Adds additional data to the job
    *
    * @param key The key of the additional data
    * @param value The data to be added
    */
    void insertOrUpdateAdditionalData(String key, String value);

    /**
     * Return the additional data stored for this job
     * @param key the key of the additional data
     * @return value The value to be found.
     */
    public String getAdditionalData(String key);

}
