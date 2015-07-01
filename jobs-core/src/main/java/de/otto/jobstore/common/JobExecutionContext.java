package de.otto.jobstore.common;

import de.otto.jobstore.service.exception.JobExecutionAbortedException;
import de.otto.jobstore.service.exception.JobExecutionTimeoutException;

import java.util.Map;

public class JobExecutionContext {

    private final String id;
    private final JobLogger jobLogger;
    private final JobExecutionPriority executionPriority;
    private final JobDefinition jobDefinition;
    private final JobInfoCache jobInfoCache;

    private volatile ResultCode resultCode = ResultCode.SUCCESSFUL;
    private String resultMessage;

    public JobExecutionContext(String id, JobLogger jobLogger, JobInfoCache jobInfoCache, JobExecutionPriority executionPriority, JobDefinition jobDefinition) {
        this.id = id;
        this.jobLogger = jobLogger;
        this.jobInfoCache = jobInfoCache;
        this.executionPriority = executionPriority;
        this.jobDefinition = jobDefinition;
    }

    public JobLogger getJobLogger() {
        return jobLogger;
    }

    public JobExecutionPriority getExecutionPriority() {
        return executionPriority;
    }

    public void setResultCode(ResultCode resultCode) {
        this.resultCode = resultCode;
    }

    public ResultCode getResultCode() {
        return resultCode;
    }

    public String getId() {
        return id;
    }

    public String getResultMessage() {
        return resultMessage;
    }

    public void setResultMessage(String resultMessage) {
        this.resultMessage = resultMessage;
    }

    /**
     * checks if conditions are met to abort the job, either an external abort request or the job reached its timeout condition
     * @throws JobExecutionAbortedException
     * @throws JobExecutionTimeoutException
     */
    public void checkForAbort() throws JobExecutionAbortedException, JobExecutionTimeoutException {
        if(jobInfoCache.isAborted()) {
            throw JobExecutionAbortedException.fromJobName(getId());
        }
        if(jobInfoCache.isTimedOut()) {
            throw JobExecutionTimeoutException.fromJobName(getId());
        }

    }

    public Map<String, String> getParameters() {
        return jobInfoCache.getParameters();
    }

    @Override
    public String toString() {
        return "JobExecutionContext{" +
                "id='" + id + '\'' +
                ", resultCode=" + resultCode +
                ", resultMessage='" + resultMessage + '\'' +
                '}';
    }

    public JobDefinition getJobDefinition() {
        return jobDefinition;
    }
}
