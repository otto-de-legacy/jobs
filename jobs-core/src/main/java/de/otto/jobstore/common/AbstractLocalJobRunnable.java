package de.otto.jobstore.common;


import de.otto.jobstore.service.exception.JobException;

/**
 *
 */
public abstract class AbstractLocalJobRunnable implements JobRunnable {

    @Override
    public final long getPollingInterval() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean isRemote() {
        return false;
    }

}
