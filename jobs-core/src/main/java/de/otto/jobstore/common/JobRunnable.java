package de.otto.jobstore.common;

import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;

/**
 * A job to be executed by a JobService {@link de.otto.jobstore.service.api.JobService}
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
     *
     * @return
     */
    long getPollingInterval();

    /**
     * Determines if the execution of a job is necessary or not.
     *
     * @return true - If execution is necessary<br/>
     *      false - If execution is not necessary
     */
    boolean isExecutionNecessary();

    /**
     *
     * @return
     */
    boolean isRemote();

    /**
     * Executes the job.
     *
     * @param jobLogger The job logger used to add additional information to a job
     * @throws de.otto.jobstore.service.exception.JobExecutionException Thrown if the execution of the job failed
     */
    void execute(JobLogger jobLogger) throws JobException;

}
