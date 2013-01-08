package de.otto.jobstore.common;

public final class ExecutionResult {

    final RunningState runningState;

    final ResultState resultState;

    public ExecutionResult(RunningState runningState) {
        this.runningState = runningState;
        this.resultState = null;
    }

    public ExecutionResult(RunningState runningState, ResultState resultState) {
        this.runningState = runningState;
        this.resultState = resultState;
    }

    public RunningState getRunningState() {
        return runningState;
    }

    public ResultState getResultState() {
        return resultState;
    }

}
