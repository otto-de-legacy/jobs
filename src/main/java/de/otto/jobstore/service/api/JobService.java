package de.otto.jobstore.service.api;

import de.otto.jobstore.common.JobRunnable;
import de.otto.jobstore.service.exception.JobNotRegisteredException;

import java.util.Set;

/**
 *  This service allows to handle multiple jobs and their associated runnables. A job has to be registered before it
 *  can be executed or cued. The service allows only one queued and running job for each distinct job name.
 *
 *  By adding running constraints it is possible to define jobs that are not allowed to run at the same time.
 */
public interface JobService {

    /**
     * Registers a job with the given name and its corresponding runnable in this job service
     *
     * @param name The name of the job to register
     * @param runnable The job runnable
     * @return true - The job was successfully registered<br>
     *     false - A job with the given name is already registered
     */
    boolean registerJob(String name, JobRunnable runnable);

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
     * Executes the job with the given name asynchronously and returns its id. The job will be queued if a job is already
     * running or running it would violate constraints. Before executing the job it is checked if its execution is necessary.
     *
     * @param name The name of the job to execute
     * @return The id of the job or null if no job could be executed/cued or no execution was necessary
     * @throws JobNotRegisteredException Thrown if no job with the given name was registered with this JobService instance
     */
    String executeJob(String name) throws JobNotRegisteredException;

    /**
     * Executes the job with the given name asynchronously and returns its id. The job will be queued if a job is already
     * running or running it would violate constraints. Before executing the job it is checked if its execution is necessary.
     *
     * @param name The name of the job to execute
     * @param forceExecution If true the job will be executed even if the result from
     * {@link de.otto.jobstore.common.JobRunnable#isExecutionNecessary()} signals that no execution is necessary
     * @return The id of the job or null if no job could be executed/cued or no execution was necessary
     * @throws JobNotRegisteredException Thrown if no job with the given name was registered with this JobService instance
     */
    String executeJob(String name, boolean forceExecution) throws JobNotRegisteredException;

    /**
     * Executes all queued jobs registered with this JobService instance asynchronously in the order they were queued.
     */
    void executeQueuedJobs();

    /**
     * Queues a job with the given name. The check if execution is necessary will be done before execution.
     *
     * @param name The name of the job to queue
     * @param forceExecution If true the job will be executed even if the result from
     * {@link de.otto.jobstore.common.JobRunnable#isExecutionNecessary()} signals that no execution is necessary
     * @return The id of the queued job or null if no job could be cued
     * @throws JobNotRegisteredException Thrown if no job with the given name was registered with this JobService instance
     */
    String queueJob(String name, boolean forceExecution) throws JobNotRegisteredException;

    /**
     * Queues a job with the given name. The check if execution is necessary will be done before execution.
     *
     * @param name The name of the job to queue
     * @return The id of the queued job or null if no job could be cued
     * @throws JobNotRegisteredException Thrown if no job with the given name was registered with this JobService instance
     */
    String queueJob(String name) throws JobNotRegisteredException;

    /**
     * Removes the queued job with the given name
     *
     * @param name The name of the queued job to remove
     */
    boolean removeQueuedJob(String name);

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
