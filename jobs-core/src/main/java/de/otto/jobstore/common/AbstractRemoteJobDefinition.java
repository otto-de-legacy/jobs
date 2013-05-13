package de.otto.jobstore.common;


public abstract class AbstractRemoteJobDefinition implements JobDefinition {


    @Override
    public long getMaxRetries() {
        return 0;
    }

    @Override
    public final boolean isRemote() {
        return true;
    }

    @Override
    public boolean isAbortable() {
        return false;
    }

}
