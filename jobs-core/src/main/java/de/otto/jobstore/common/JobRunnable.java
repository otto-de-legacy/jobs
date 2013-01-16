package de.otto.jobstore.common;

import de.otto.jobstore.service.exception.JobException;

import java.util.Map;

/**
 * A job to be executed by a JobService {@link de.otto.jobstore.service.JobService}
 */
public interface JobRunnable {

    /**
     * Returns the defining data of this job
     */
    JobDefinition getJobDefinition();

    /**
     * Returns the parameters being used to execute the job.
     */
    Map<String, String> getParameters();

    /**
     * Asks for the current remote status. Is only called on remote jobs.
     */
    RemoteJobStatus getRemoteStatus(JobExecutionContext context);

    /**
     * Checks preconditions whether job should be executed.
     *
     * @return true - The job can now be executed</br>
     *         false - The job should not be executed
     */
    boolean prepare(JobExecutionContext context) throws JobException;

    /**
     * Executes the job.
     *
     * @param context The context in which this job is executed
     * @throws de.otto.jobstore.service.exception.JobExecutionException Thrown if the execution of the job failed
     */
    void execute(JobExecutionContext context) throws JobException;

    /**
     * This method is called after the job is executed successfully.
     */
    void afterExecution(JobExecutionContext context) throws JobException;

}
