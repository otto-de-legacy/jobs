package de.otto.jobstore.common;


public abstract class AbstractRemoteJobDefinition implements JobDefinition {

    @Override
    public final boolean isRemote() {
        return true;
    }

}
