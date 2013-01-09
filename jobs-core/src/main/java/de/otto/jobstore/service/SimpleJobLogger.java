package de.otto.jobstore.service;

import de.otto.jobstore.common.JobLogger;
import de.otto.jobstore.repository.JobInfoRepository;

import java.util.ArrayList;
import java.util.List;


final class SimpleJobLogger implements JobLogger {

    private final String jobName;
    private final JobInfoRepository jobInfoRepository;
    private List<String> logLines;

    SimpleJobLogger(String jobName, JobInfoRepository jobInfoRepository) {
        this.jobName = jobName;
        this.jobInfoRepository = jobInfoRepository;
        this.logLines = new ArrayList<String>();
    }

    @Override
    public void addLoggingData(String logLine) {
        if (logLine != null && logLine.trim().length() > 0) {
            jobInfoRepository.addLogLine(jobName, logLine);
            logLines.add(logLine);
        }
    }

    @Override
    public List<String> getLoggingData() {
        return logLines;
    }

    @Override
    public void insertOrUpdateAdditionalData(String key, String value) {
        jobInfoRepository.addAdditionalData(jobName, key, value);
    }

}
