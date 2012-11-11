package de.otto.jobstore.common;

public interface JobRunnable {

    long getMaxExecutionTime();

    boolean isExecutionNecessary();

    void execute(JobLogger jobLogger) throws Exception;

    // TODO: über MBean steuern, ob eine Ausführung ausgesetzt werden soll
    //boolean isActive();

}
