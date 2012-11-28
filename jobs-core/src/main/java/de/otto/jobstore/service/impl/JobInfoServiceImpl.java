package de.otto.jobstore.service.impl;

import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.ResultState;
import de.otto.jobstore.repository.api.JobInfoRepository;
import de.otto.jobstore.service.api.JobInfoService;

import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;

public final class JobInfoServiceImpl implements JobInfoService {

    private final JobInfoRepository jobInfoRepository;

    public JobInfoServiceImpl(JobInfoRepository jobInfoRepository) {
        this.jobInfoRepository = jobInfoRepository;
    }

    @Override
    public JobInfo getMostRecentExecuted(String name) {
        return jobInfoRepository.findMostRecentByNameAndResultState(name,
                EnumSet.complementOf(EnumSet.of(ResultState.NOT_EXECUTED)));
    }

    @Override
    public JobInfo getMostRecentSuccessful(String name) {
        return jobInfoRepository.findMostRecentByNameAndResultState(name, EnumSet.of(ResultState.SUCCESSFUL));
    }

    @Override
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

    @Override
    public List<JobInfo> getByName(String name) {
        return jobInfoRepository.findByName(name, null);
    }

    @Override
    public List<JobInfo> getByName(String name, Integer limit) {
        return jobInfoRepository.findByName(name, limit);
    }

    @Override
    public JobInfo getById(String id) {
        return jobInfoRepository.findById(id);
    }

    @Override
    public List<JobInfo> getByNameAndTimeRange(String name, Date after) {
        return jobInfoRepository.findByNameAndTimeRange(name, after, null);
    }

    @Override
    public List<JobInfo> getByNameAndTimeRange(String name, Date after, Date before) {
        return jobInfoRepository.findByNameAndTimeRange(name, after, before);
    }

    @Override
    public void clean() {
        jobInfoRepository.clear(false);
    }

}
