package de.otto.jobstore.common;

import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;

/**
 * A job to be executed by a JobService {@link de.otto.jobstore.service.JobService}
 */
public interface JobRunnable {

    /**
     * The name of the job
     *
     * @return The name of the job
     */
    String getName();

    /**
     * The time after which a job is considered to be timed out.
     *
     * @return The time in milliseconds
     */
    long getMaxExecutionTime();

    /**
     * The interval after which the job should be polled for new information
     *
     * @return The interval in milliseconds
     */
    long getPollingInterval();

    /**
     * Flag if the job is executed locally or remotely
     *
     * @return true - The job is executed remotely</br>
     *          false - The job is executed locally
     */
    boolean isRemote();

    /**
     * Executes the job.
     *
     * @param context The context in which this job is executed
     * @throws de.otto.jobstore.service.exception.JobExecutionException Thrown if the execution of the job failed
     */
    void execute(JobExecutionContext context) throws JobException;

    /**
     * This method is called once the job is executed successfully
     */
    public void executeOnSuccess() throws JobException;

}
