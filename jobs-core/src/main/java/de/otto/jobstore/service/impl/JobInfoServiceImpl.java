package de.otto.jobstore.service.impl;

import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.ResultState;
import de.otto.jobstore.repository.api.JobInfoRepository;
import de.otto.jobstore.service.api.JobInfoService;

import java.util.EnumSet;

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
    public void clean() {
        jobInfoRepository.clear(false);
    }

}
