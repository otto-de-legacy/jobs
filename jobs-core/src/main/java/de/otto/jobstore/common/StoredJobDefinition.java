package de.otto.jobstore.common;


import com.mongodb.DBObject;
import de.otto.jobstore.common.properties.JobDefinitionProperty;

public final class StoredJobDefinition extends AbstractItem implements JobDefinition {

    public static final StoredJobDefinition JOB_EXEC_SEMAPHORE = new StoredJobDefinition("ALL_JOBS", 0, 0, 0, false, false);

    private static final long serialVersionUID = 2454224305569320787L;

    public StoredJobDefinition(DBObject dbObject) {
        super(dbObject);
    }

    /*
    @Deprecated
    public StoredJobDefinition(String name, long maxIdleTime, long pollingInterval, boolean remote, boolean abortable) {
        this(name, maxIdleTime, pollingInterval, 0, remote, abortable);
    }
    */

    public StoredJobDefinition(String name, long maxIdleTime, long pollingInterval, long retries, boolean remote, boolean abortable) {
        addProperty(JobDefinitionProperty.NAME, name);
        addProperty(JobDefinitionProperty.MAX_IDLE_TIME, maxIdleTime);
        addProperty(JobDefinitionProperty.POLLING_INTERVAL, pollingInterval);
        addProperty(JobDefinitionProperty.RETRIES, retries);
        addProperty(JobDefinitionProperty.REMOTE, remote);
        addProperty(JobDefinitionProperty.ABORTABLE, abortable);
    }

    public StoredJobDefinition(JobDefinition jd) {
        this(jd.getName(), jd.getMaxIdleTime(), jd.getPollingInterval(), jd.getRetries(), jd.isRemote(), jd.isAbortable());
    }

    public String getName() {
        return getProperty(JobDefinitionProperty.NAME);
    }



    public long getMaxIdleTime() {
        Long maxIdleTime = getProperty(JobDefinitionProperty.MAX_IDLE_TIME);
        if(maxIdleTime == null){
            maxIdleTime = getProperty(JobDefinitionProperty.TIMEOUT_PERIOD);
        }
        return maxIdleTime;
    }

    public long getMaxExecutionTime() {
        Long maxExecutionTime = getProperty(JobDefinitionProperty.MAX_EXECUTION_TIME);
        if(maxExecutionTime == null){
            maxExecutionTime = Long.valueOf(1000*60*60*2);
        }
        return maxExecutionTime;
    }

    public long getPollingInterval() {
        return getProperty(JobDefinitionProperty.POLLING_INTERVAL);
    }

    public long getRetries() {
        return getProperty(JobDefinitionProperty.RETRIES);
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

}
