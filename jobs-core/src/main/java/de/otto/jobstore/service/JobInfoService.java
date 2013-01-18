package de.otto.jobstore.service;

import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.ResultCode;
import de.otto.jobstore.common.RunningState;
import de.otto.jobstore.repository.JobInfoRepository;

import java.util.*;

/**
 * This service gives access to information on the jobs that have been executed. This allows for example to make
 * decisions on whether to execute another job if the previous one timed out or failed.
 */
public class JobInfoService {

    private final JobInfoRepository jobInfoRepository;

    public JobInfoService(JobInfoRepository jobInfoRepository) {
        this.jobInfoRepository = jobInfoRepository;
    }

    /**
     * Returns the information on the most recent job that has been executed.
     *
     * @param name The name of the job for which to return the information
     * @return The most recent executed job
     */
    public JobInfo getMostRecentExecuted(String name) {
        return jobInfoRepository.findMostRecentFinished(name);
    }

    /**
     * Returns the information on the most recent job that has been successfully executed.
     *
     * @param name The name of the job for which to return the information
     * @return The most recent successfully executed job
     */
    public JobInfo getMostRecentSuccessful(String name) {
        return jobInfoRepository.findMostRecentByNameAndResultState(name, EnumSet.of(ResultCode.SUCCESSFUL));
    }

    /**
     * Returns for each job name the information on the most recent job that has been executed
     * @return The list of job information
     */
    public List<JobInfo> getMostRecentExecuted() {
        final List<String> names = jobInfoRepository.distinctJobNames();
        final List<JobInfo> jobInfoList = new ArrayList<>();
        for (String name : names) {
            final JobInfo jobInfo = getMostRecentExecuted(name);
            if (jobInfo != null) {
                jobInfoList.add(jobInfo);
            }
        }
        return jobInfoList;
    }

    /**
     * Returns for the given name all job information sorted descending by the creation time of the jobs.
     *
     * @param name The name of the job for which to return the information
     * @return The list of job information
     */
    public List<JobInfo> getByName(String name) {
        return jobInfoRepository.findByName(name, null);
    }

    /**
     * Returns for the given name the job with the given running state or null if none exists
     *
     * @param name The name of the job for which to return the information
     * @param runningState The running state the job to return
     * @return The job with the given name and running state, or null
     */
    public JobInfo getByNameAndRunningState(String name, RunningState runningState) {
        return jobInfoRepository.findByNameAndRunningState(name, runningState);
    }

    /**
     * Returns for the given name all job information sorted descending by the creation time of the jobs.
     *
     * @param name The name of the job for which to return the information
     * @param limit The maximum number of elements to return
     * @return The list of job information
     */
    public List<JobInfo> getByName(String name, Integer limit) {
        return jobInfoRepository.findByName(name, limit);
    }

    /**
     * Returns for the given id the job information
     *
     * @param id The id of the job for which to return the information
     * @return The job information or null if it does not exist
     */
    public JobInfo getById(String id) {
        return jobInfoRepository.findById(id);
    }

    /**
     * Returns all job information for the given name which were last modified after the given after date and before
     * the given before date. The result list is sorted descending by the jobs creation date.
     *
     * @param name The name of the job for which to return the information
     * @param after The date after which the last modified date has to be
     * @param before The date before which the last modified date has to be
     * @return The list of job information
     */
    public List<JobInfo> getByNameAndTimeRange(String name, Date after, Date before, Set<ResultCode> resultCodes) {
        return jobInfoRepository.findByNameAndTimeRange(name, after, before, resultCodes);
    }

    /**
     * Remove all job information.
     */
    public void clean() {
        jobInfoRepository.clear(false);
    }

}
