package de.otto.jobstore.common;

import de.otto.jobstore.service.exception.JobException;

import java.util.Collections;
import java.util.Map;

public abstract class AbstractLocalJobRunnable implements JobRunnable {

    @Override
    public final long getPollingInterval() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean isRemote() {
        return false;
    }

    @Override
    public Map<String, String> getParameters() {
        return Collections.emptyMap();
    }

    /**
     * By default returns true, override for your custom needs,
     */
    @Override
    public boolean prepare(JobExecutionContext context) {
        return true;
    }

    @Override
    public void afterExecution(JobExecutionContext context) throws JobException {}

}
