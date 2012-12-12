package de.otto.jobstore.common;


/**
 *
 */
public abstract class AbstractLocalJobRunnable implements JobRunnable {

    @Override
    public final long getPollingInterval() {
        return 0;
    }

    @Override
    public final boolean isRemote() {
        return false;
    }

}
