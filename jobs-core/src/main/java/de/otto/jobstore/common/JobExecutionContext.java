package de.otto.jobstore.common;

public final class JobExecutionContext {

    private final String id;
    private final JobLogger jobLogger;
    private final JobExecutionPriority executionPriority;
    private final JobInfoCache jobInfoCache;

    private volatile ResultCode resultCode = ResultCode.SUCCESSFUL;
    private String resultMessage;

    public JobExecutionContext(String id, JobLogger jobLogger, JobInfoCache jobInfoCache, JobExecutionPriority executionPriority) {
        this.id = id;
        this.jobLogger = jobLogger;
        this.jobInfoCache = jobInfoCache;
        this.executionPriority = executionPriority;
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

    public boolean isAborted() {
        return jobInfoCache.isAborted();
    }

    @Override
    public String toString() {
        return "JobExecutionContext{" +
                "id='" + id + '\'' +
                ", resultCode=" + resultCode +
                ", resultMessage='" + resultMessage + '\'' +
                '}';
    }

}
