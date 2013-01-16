package de.otto.jobstore.common;


import com.mongodb.DBObject;
import de.otto.jobstore.common.properties.JobDefinitionProperty;

public final class StoredJobDefinition extends AbstractItem implements JobDefinition {

    private static final long serialVersionUID = 2454224305569320787L;

    public StoredJobDefinition(DBObject dbObject) {
        super(dbObject);
    }

    public StoredJobDefinition(String name, long timeoutPeriod, long pollingInterval, boolean remote) {
        addProperty(JobDefinitionProperty.NAME, name);
        addProperty(JobDefinitionProperty.TIMEOUT_PERIOD, timeoutPeriod);
        addProperty(JobDefinitionProperty.POLLING_INTERVAL, pollingInterval);
        addProperty(JobDefinitionProperty.REMOTE, remote);
    }

    public StoredJobDefinition(JobDefinition jd) {
        this(jd.getName(), jd.getTimeoutPeriod(), jd.getPollingInterval(), jd.isRemote());
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

    public void setDisabled(boolean paused) {
        addProperty(JobDefinitionProperty.DISABLED, paused);
    }

    public boolean isDisabled() {
        final Boolean disabled = getProperty(JobDefinitionProperty.DISABLED);
        return disabled == null ? false : disabled;
    }

}
