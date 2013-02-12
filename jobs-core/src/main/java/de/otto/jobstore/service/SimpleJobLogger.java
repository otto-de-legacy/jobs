package de.otto.jobstore.service;

import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.JobLogger;
import de.otto.jobstore.common.RunningState;
import de.otto.jobstore.repository.JobInfoRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class SimpleJobLogger implements JobLogger {

    private final String jobId;
    private final String jobName;
    private final JobInfoRepository jobInfoRepository;
    private List<String> logLines;

    SimpleJobLogger(String jobId, String jobName, JobInfoRepository jobInfoRepository) {
        this(jobId, jobName, jobInfoRepository, new ArrayList<String>());
    }

    SimpleJobLogger(String jobId, String jobName, JobInfoRepository jobInfoRepository, List<String> logLines) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.jobInfoRepository = jobInfoRepository;
        this.logLines = logLines == null ? new ArrayList<String>() : logLines;
    }

    @Override
    public void addLoggingData(String logLine) {
        if (logLine != null && logLine.trim().length() > 0) {
            jobInfoRepository.addLogLine(jobId, logLine);
            logLines.add(logLine);
        }
    }

    @Override
    public List<String> getLoggingData() {
        return logLines;
    }

    @Override
    public void insertOrUpdateAdditionalData(String key, String value) {
        jobInfoRepository.addAdditionalData(jobId, key, value);
    }

    @Override
    public String getAdditionalData(String key) {
        final JobInfo jobInfo = jobInfoRepository.findById(jobId);
        Map<String, String> additionalData = jobInfo.getAdditionalData();
        return additionalData == null ? null : additionalData.get(key);
    }

}
