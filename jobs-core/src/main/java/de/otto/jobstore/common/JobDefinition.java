package de.otto.jobstore.common;

public interface JobDefinition {

    /**
     * The name of the job
     */
    String getName();


    /**
     * The time after which a job is considered to be timed out because it has not updated anymore  (in milliseconds).
     */
    long getMaxIdleTime();


    /**
     * The time after which a job is considered to be timed out because its total runningtime has exceeded (in milliseconds).
     */
    long getMaxExecutionTime();

    /**
     * The interval after which the job should be polled for new information (in milliseconds).
     */
    long getPollingInterval();

    /**
     * Flag if the job is executed locally or remotely
     *
     * @return true - The job is executed remotely</br>
     *         false - The job is executed locally
     */
    boolean isRemote();

    /**
     * Flag if the job can be aborted
     *
     * @return true - The job can be aborted</br>
     *         false - The job cannot be aborted
     */
    boolean isAbortable();

}
