package de.otto.jobstore.common;

public interface JobDefinition {

    /**
     * The name of the job
     */
    String getName();

    /**
     * The time after which a job is considered to be timed out (in milliseconds).
     */
    long getTimeoutPeriod();

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
