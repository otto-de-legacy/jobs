package de.otto.jobstore.service.api;

import de.otto.jobstore.common.JobInfo;

/**
 * This service gives access to information on the jobs that have been executed. This allows for example to make
 * decisions on whether to execute another job if the previous one timed out or failed.
 */
public interface JobInfoService {

    /**
     * Returns the most recent job that has been executed.
     *
     * @param name The name of the job to return
     * @return The most recent executed job
     */
    JobInfo getMostRecentExecuted(String name);

    /**
     * Returns the most recent job that has been successfully executed.
     *
     * @param name The name of the job to return
     * @return The most recent successfully executed job
     */
    JobInfo getMostRecentSuccessful(String name);

    /**
     * Removed all job information.
     */
    void clean();

}
