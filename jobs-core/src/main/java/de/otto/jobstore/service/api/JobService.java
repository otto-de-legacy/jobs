package de.otto.jobstore.service.api;

import de.otto.jobstore.common.JobRunnable;
import de.otto.jobstore.service.exception.*;

import java.util.Set;

/**
 *  This service allows to handle multiple jobs and their associated runnables. A job has to be registered before it
 *  can be executed or cued. The service allows only one queued and one running job for each distinct job name.
 *
 *  In order to execute jobs they have to be queued and afterwards executed by callings {#executeQueuedJobs}. By adding
 *  running constraints it is possible to define jobs that are not allowed to run at the same time.
 */
public interface JobService {

    /**
     * Registers a job with the given runnable in this job service
     *
     * @param runnable The job runnable
     * @return true - The job was successfully registered<br>
     *     false - A job with the given name is already registered
     */
    boolean registerJob(JobRunnable runnable);

    /**
     * Adds a running constraint to this JobService instance.
     *
     * @param constraint The names of the jobs that are not allowed to run at the same time
     * @return true - If the running constraint was successfully added<br>
     *     false - If the running constraint already exists
     * @throws de.otto.jobstore.service.exception.JobNotRegisteredException Thrown if the constraint contains a name of
     * a job which is not registered with this JobService instance
     */
    boolean addRunningConstraint(Set<String> constraint) throws JobNotRegisteredException;

    /**
     * Executes a job with the given name and returns its ID. If a job is already running or running it would violate
     * running constraints it this job will be added to the queue. If a job is already queued an exception will be thrown.
     *
     * @param name The name of the job to execute
     * @param forceExecution If true the job will be executed even if the result from
     * {@link de.otto.jobstore.common.JobRunnable#isExecutionNecessary()} signals that no execution is necessary
     * @return The id of the executing or queued job
     * @throws JobNotRegisteredException Thrown if no job with the given name was registered with this JobService instance
     * @throws JobAlreadyQueuedException If a job with the given name is already queued for execution or another
     * JobService instance queued the job while this method was executed
     * @throws JobAlreadyRunningException If another JobService instance executed a job with the given name while this
     * method was executed
     * @throws JobExecutionNotNecessaryException If the execution of the job was not necessary
     * @throws JobExecutionDisabledException If job execution has been disabled
     */
    String executeJob(String name, boolean forceExecution) throws JobNotRegisteredException, JobAlreadyQueuedException,
            JobAlreadyRunningException, JobExecutionNotNecessaryException, JobExecutionDisabledException;

    /**
     * Executes a job with the given name and returns its ID. If a job is already running or running it would violate
     * running constraints it this job will be added to the queue. If a job is already queued an exception will be thrown.
     *
     * @param name The name of the job to execute
     * {@link de.otto.jobstore.common.JobRunnable#isExecutionNecessary()} signals that no execution is necessary
     * @return The id of the executing or queued job
     * @throws JobNotRegisteredException Thrown if no job with the given name was registered with this JobService instance
     * @throws JobAlreadyQueuedException If a job with the given name is already queued for execution or another
     * JobService instance queued the job while this method was executed
     * @throws JobAlreadyRunningException If another JobService instance executed a job with the given name while this
     * method was executed
     * @throws JobExecutionNotNecessaryException If the execution of the job was not necessary
     * @throws JobExecutionDisabledException If job execution has been disabled
     */
    String executeJob(String name) throws JobNotRegisteredException, JobAlreadyQueuedException,
            JobAlreadyRunningException, JobExecutionNotNecessaryException, JobExecutionDisabledException;


    /**
     * Removes a job with the given from the queue and sets its result state to not executed.
     *
     * @param name The name of the job
     * @return true - If a queued job with the given name was found</br>
     *         false - If no queued job with the given name could be found
     *
     */
    boolean removeJobFromQueue(String name);

    /**
     * Executes all queued jobs registered with this JobService instance asynchronously in the order they were queued.
     */
    void executeQueuedJobs();

    /**
     *
     */
    void pollRemoteJobs();

    /**
     * Removed all registered jobs and constraints from the JobService instance
     */
    void clean();

    /**
     * Returns the Names of all registered jobs
     *
     * @return The set of names registered with the JobService instance
     */
    Set<String> listJobNames();

    /**
     * Returns the Set of all constraints
     *
     * @return The set of constraints registered with the JobService instance
     */
    Set<Set<String>> listRunningConstraints();

    /**
     * Stops all jobs registered with this JobService and running on this host.
     */
    void shutdownJobs();
}
