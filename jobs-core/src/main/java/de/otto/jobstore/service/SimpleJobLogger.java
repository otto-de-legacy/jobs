package de.otto.jobstore.service;

import de.otto.jobstore.common.JobLogger;
import de.otto.jobstore.repository.JobInfoRepository;


final class SimpleJobLogger implements JobLogger {

    private final String jobName;
    private final JobInfoRepository jobInfoRepository;

    SimpleJobLogger(String jobName, JobInfoRepository jobInfoRepository) {
        this.jobName = jobName;
        this.jobInfoRepository = jobInfoRepository;
    }

    @Override
    public void addLoggingData(String log) {
        if (log != null) {
            jobInfoRepository.addLogLine(jobName, log);
        }
    }

    @Override
    public void insertOrUpdateAdditionalData(String key, String value) {
        jobInfoRepository.addAdditionalData(jobName, key, value);
    }

}
