package de.otto.jobstore.common;

/**
 *
 */
public interface JobRunnable {

    /**
     *
     * @return
     */
    long getMaxExecutionTime();

    /**
     *
     * @return
     */
    boolean isExecutionNecessary();

    /**
     *
     * @param jobLogger
     * @throws Exception
     */
    void execute(JobLogger jobLogger) throws Exception;

}
