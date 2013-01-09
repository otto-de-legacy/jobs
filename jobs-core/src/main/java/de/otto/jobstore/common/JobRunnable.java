package de.otto.jobstore.common;

import de.otto.jobstore.service.exception.JobException;

import java.util.Map;

/**
 * A job to be executed by a JobService {@link de.otto.jobstore.service.JobService}
 */
public interface JobRunnable {

    /**
     * The ID uniquely identifying this job instance.
     */
    String getId();
    void setId(String id);

    /**
     * The name of the job
     */
    String getName();

    /**
     * Returns the parameters being used to execute the job.
     */
    Map<String, String> getParameters();

    /**
     * The time after which a job is considered to be timed out (in milliseconds).
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
     * Executes the job.
     *
     * @param context The context in which this job is executed
     * @throws de.otto.jobstore.service.exception.JobExecutionException Thrown if the execution of the job failed
     */
    JobExecutionResult execute(JobExecutionContext context) throws JobException;

    /**
     * This method is called right before the job is executed.
     */
    void executeOnStart(JobLogger jobLogger) throws JobException;

    /**
     * This method is called once the job is executed successfully.
     */
    void executeOnSuccess(JobLogger jobLogger) throws JobException;

}
