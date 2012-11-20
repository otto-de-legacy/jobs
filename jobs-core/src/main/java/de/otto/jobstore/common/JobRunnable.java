package de.otto.jobstore.common;

/**
 * A job to be executed by a JobService {@link de.otto.jobstore.service.api.JobService}
 */
public interface JobRunnable {

    /**
     * The time after which a job is considered to be timed out.
     *
     * @return The time in milliseconds
     */
    long getMaxExecutionTime();

    /**
     * Determines if the execution of a job is necessary or not.
     *
     * @return true - If execution is necessary<br/>
     *      false - If execution is not necessary
     */
    boolean isExecutionNecessary();

    /**
     * Executes the job.
     *
     * @param jobLogger The job logger used to add additional information to a job
     * @throws Exception Thrown if the execution of the job failed
     */
    void execute(JobLogger jobLogger) throws Exception;

}
