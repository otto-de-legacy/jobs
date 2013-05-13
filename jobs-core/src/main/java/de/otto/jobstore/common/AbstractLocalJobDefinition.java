package de.otto.jobstore.common;

public abstract class AbstractLocalJobDefinition implements JobDefinition {

    @Override
    public final long getPollingInterval() {
        return -1;
    }

    @Override
    public long getRetries() {
        return 0;
    }

    @Override
    public final boolean isRemote() {
        return false;
    }

    @Override
    public boolean isAbortable() {
        return false;
    }


}
