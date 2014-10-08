package de.otto.jobstore.common;


import com.mongodb.DBObject;
import de.otto.jobstore.common.properties.JobDefinitionProperty;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public final class StoredJobDefinition extends AbstractItem implements JobDefinition {

    public static final StoredJobDefinition JOB_EXEC_SEMAPHORE = new StoredJobDefinition("ALL_JOBS", 0, 0, 0, 0, 0, false, false);

    private static final long serialVersionUID = 2454224305569320787L;

    public StoredJobDefinition(DBObject dbObject) {
        super(dbObject);
    }

    public StoredJobDefinition(String name, long maxIdleTime, long maxExecutionTime, long pollingInterval, long maxRetries, long retryInterval, boolean remote, boolean abortable) {
        addProperty(JobDefinitionProperty.NAME, name);
        addProperty(JobDefinitionProperty.MAX_IDLE_TIME, maxIdleTime);
        addProperty(JobDefinitionProperty.MAX_EXECUTION_TIME, maxExecutionTime);
        addProperty(JobDefinitionProperty.POLLING_INTERVAL, pollingInterval);
        addProperty(JobDefinitionProperty.MAX_RETRIES, maxRetries);
        addProperty(JobDefinitionProperty.RETRY_INTERVAL, retryInterval);
        addProperty(JobDefinitionProperty.REMOTE, remote);
        addProperty(JobDefinitionProperty.ABORTABLE, abortable);
    }

    public StoredJobDefinition(JobDefinition jd) {
        this(jd.getName(), jd.getMaxIdleTime(), jd.getMaxExecutionTime(), jd.getPollingInterval(), jd.getMaxRetries(), jd.getRetryInterval(), jd.isRemote(), jd.isAbortable());
    }

    public String getName() {
        return getProperty(JobDefinitionProperty.NAME);
    }



    public long getMaxIdleTime() {
        return getProperty(JobDefinitionProperty.MAX_IDLE_TIME);
    }

    public long getMaxExecutionTime() {
        Long maxExecutionTime = getProperty(JobDefinitionProperty.MAX_EXECUTION_TIME);
        if(maxExecutionTime == null){
            maxExecutionTime = TimeUnit.HOURS.toMillis(2);
        }
        return maxExecutionTime;
    }

    public long getPollingInterval() {
        return getProperty(JobDefinitionProperty.POLLING_INTERVAL);
    }

    public long getMaxRetries() {
        return getProperty(JobDefinitionProperty.MAX_RETRIES);
    }

    public long getRetryInterval() {
        return getProperty(JobDefinitionProperty.RETRY_INTERVAL);
    }

    public boolean isRemote() {
        final Boolean remote = getProperty(JobDefinitionProperty.REMOTE);
        return remote == null ? false : remote;
    }

    public boolean isAbortable() {
        final Boolean abortable = getProperty(JobDefinitionProperty.ABORTABLE);
        return abortable == null ? false : abortable;
    }

    public void setDisabled(boolean disabled) {
        addProperty(JobDefinitionProperty.DISABLED, disabled);
    }

    public boolean isDisabled() {
        final Boolean disabled = getProperty(JobDefinitionProperty.DISABLED);
        return disabled == null ? false : disabled;
    }

    public Date getLastNotExecuted() {
        return getProperty(JobDefinitionProperty.LAST_NOT_EXECUTED);
    }

}
