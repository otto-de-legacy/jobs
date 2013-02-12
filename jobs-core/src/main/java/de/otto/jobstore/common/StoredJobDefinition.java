package de.otto.jobstore.common;


import com.mongodb.DBObject;
import de.otto.jobstore.common.properties.JobDefinitionProperty;

public final class StoredJobDefinition extends AbstractItem implements JobDefinition {

    public static final StoredJobDefinition JOB_EXEC_SEMAPHORE = new StoredJobDefinition("ALL_JOBS", 0, 0, false, false);

    private static final long serialVersionUID = 2454224305569320787L;

    public StoredJobDefinition(DBObject dbObject) {
        super(dbObject);
    }

    public StoredJobDefinition(String name, long timeoutPeriod, long pollingInterval, boolean remote, boolean abortable) {
        addProperty(JobDefinitionProperty.NAME, name);
        addProperty(JobDefinitionProperty.TIMEOUT_PERIOD, timeoutPeriod);
        addProperty(JobDefinitionProperty.POLLING_INTERVAL, pollingInterval);
        addProperty(JobDefinitionProperty.REMOTE, remote);
        addProperty(JobDefinitionProperty.ABORTABLE, abortable);
    }

    public StoredJobDefinition(JobDefinition jd) {
        this(jd.getName(), jd.getTimeoutPeriod(), jd.getPollingInterval(), jd.isRemote(), jd.isAbortable());
    }

    public String getName() {
        return getProperty(JobDefinitionProperty.NAME);
    }

    public long getTimeoutPeriod() {
        return getProperty(JobDefinitionProperty.TIMEOUT_PERIOD);
    }

    public long getPollingInterval() {
        return getProperty(JobDefinitionProperty.POLLING_INTERVAL);
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
