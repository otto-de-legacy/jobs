package de.otto.jobstore.common;

import de.otto.jobstore.repository.JobDefinitionRepository;

public class JobDefinitionCache {

    private static int UPDATE_INTERVAL = 10000;
    private final String name;
    private final JobDefinitionRepository jobDefinitionRepository;
    private volatile long lastUpdate = 0;
    private volatile StoredJobDefinition storedJobDefinition;

    public JobDefinitionCache(String name, JobDefinitionRepository jobDefinitionRepository) {
        this.name = name;
        this.jobDefinitionRepository = jobDefinitionRepository;
        this.storedJobDefinition = getJobDefinition();
    }

    public boolean isAborted() {
        return getJobDefinition().isAborted();
    }

    private StoredJobDefinition getJobDefinition() {
        final long currentTime = System.currentTimeMillis();
        if (lastUpdate + UPDATE_INTERVAL < currentTime) {
            synchronized (this) {
                if (lastUpdate + UPDATE_INTERVAL < currentTime) {
                    lastUpdate = currentTime;
                    storedJobDefinition = jobDefinitionRepository.find(name);
                }
            }
        }
        return storedJobDefinition;
    }

    public static void setUpdateInterval(int updateInterval) {
        JobDefinitionCache.UPDATE_INTERVAL = updateInterval;
    }

}
