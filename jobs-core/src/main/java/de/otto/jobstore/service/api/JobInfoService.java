package de.otto.jobstore.service.api;

import de.otto.jobstore.common.JobInfo;

import java.util.Date;
import java.util.List;

/**
 * This service gives access to information on the jobs that have been executed. This allows for example to make
 * decisions on whether to execute another job if the previous one timed out or failed.
 */
public interface JobInfoService {

    /**
     * Returns the information on the most recent job that has been executed.
     *
     * @param name The name of the job for which to return the information
     * @return The most recent executed job
     */
    JobInfo getMostRecentExecuted(String name);

    /**
     * Returns the information on the most recent job that has been successfully executed.
     *
     * @param name The name of the job for which to return the information
     * @return The most recent successfully executed job
     */
    JobInfo getMostRecentSuccessful(String name);

    /**
     * Returns for each job name the information on the most recent job that has been executed
     * @return The list of job information
     */
    List<JobInfo> getMostRecentExecuted();

    /**
     * Returns for the given name all job information sorted descending by the creation time of the jobs.
     *
     * @param name The name of the job for which to return the information
     * @return The list of job information
     */
    List<JobInfo> getByName(String name);

    /**
     * Returns for the given name all job information sorted descending by the creation time of the jobs.
     *
     * @param name The name of the job for which to return the information
     * @param limit The maximum number of elements to return
     * @return The list of job information
     */
    List<JobInfo> getByName(String name, Integer limit);

    /**
     * Returns for the given id the job information
     *
     * @param id The id of the job for which to return the information
     * @return The job information or null if it does not exist
     */
    JobInfo getById(String id);

    /**
     * Returns all job information for the given name which were last modified after the given date. The result list
     * is sorted descending by the jobs creation date.
     *
     * @param name The name of the job for which to return the information
     * @param after The date after which the last modified date has to be
     * @return The list of job information
     */
    List<JobInfo> getByNameAndTimeRange(String name, Date after);

    /**
     * Returns all job information for the given name which were last modified after the given after date and before
     * the given before date. The result list is sorted descending by the jobs creation date.
     *
     * @param name The name of the job for which to return the information
     * @param after The date after which the last modified date has to be
     * @param before The date before which the last modified date has to be
     * @return The list of job information
     */
    List<JobInfo> getByNameAndTimeRange(String name, Date after, Date before);

    /**
     * Removed all job information.
     */
    void clean();

}
