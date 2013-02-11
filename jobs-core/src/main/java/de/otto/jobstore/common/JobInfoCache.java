package de.otto.jobstore.common;

import de.otto.jobstore.repository.JobInfoRepository;

public class JobInfoCache {

    private static int UPDATE_INTERVAL = 10000;
    private final String id;
    private final JobInfoRepository jobInfoRepository;
    private volatile long lastUpdate = 0;
    private volatile JobInfo jobInfo;

    public JobInfoCache(String id, JobInfoRepository jobDefinitionRepository) {
        this.id = id;
        this.jobInfoRepository = jobDefinitionRepository;
        this.jobInfo = getJobInfo();
    }

    public boolean isAborted() {
        return getJobInfo().isAborted();
    }

    private JobInfo getJobInfo() {
        final long currentTime = System.currentTimeMillis();
        if (lastUpdate + UPDATE_INTERVAL < currentTime) {
            synchronized (this) {
                if (lastUpdate + UPDATE_INTERVAL < currentTime) {
                    lastUpdate = currentTime;
                    jobInfo = jobInfoRepository.findById(id);
                }
            }
        }
        return jobInfo;
    }

    public static void setUpdateInterval(int updateInterval) {
        JobInfoCache.UPDATE_INTERVAL = updateInterval;
    }

}
