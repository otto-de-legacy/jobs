package de.otto.jobstore.common;

public final class JobExecutionContext {

    private final String id;
    private final JobLogger jobLogger;
    private final JobExecutionPriority executionPriority;

    private RunningState runningState;
    private ResultCode resultCode;
    private String resultMessage;

    public JobExecutionContext(String id, JobLogger jobLogger, JobExecutionPriority executionPriority) {
        this.id = id;
        this.jobLogger = jobLogger;
        this.executionPriority = executionPriority;
        this.runningState = RunningState.QUEUED;
    }

    public JobLogger getJobLogger() {
        return jobLogger;
    }

    public JobExecutionPriority getExecutionPriority() {
        return executionPriority;
    }

    public RunningState getRunningState() {
        return runningState;
    }

    public void setRunningState(RunningState state) {
        this.runningState = state;
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

}
