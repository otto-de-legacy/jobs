package de.otto.jobstore.common;

public final class JobExecutionResult {

    final RunningState runningState;

    final ResultCode resultCode;

    public JobExecutionResult(RunningState runningState) {
        this.runningState = runningState;
        this.resultCode = null;
    }

    public JobExecutionResult(RunningState runningState, ResultCode resultState) {
        this.runningState = runningState;
        this.resultCode = resultState;
    }

    public RunningState getRunningState() {
        return runningState;
    }

    public ResultCode getResultCode() {
        return resultCode;
    }

}
