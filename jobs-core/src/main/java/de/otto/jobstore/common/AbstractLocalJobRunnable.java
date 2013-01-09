package de.otto.jobstore.common;

import de.otto.jobstore.service.exception.JobException;

public abstract class AbstractLocalJobRunnable implements JobRunnable {

    protected String id;

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public final long getPollingInterval() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean isRemote() {
        return false;
    }

    @Override
    public void executeOnStart(JobLogger jobLogger) throws JobException {}

    @Override
    public void executeOnSuccess(JobLogger jobLogger) throws JobException {}

}
