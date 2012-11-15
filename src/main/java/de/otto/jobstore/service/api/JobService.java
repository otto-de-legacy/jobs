package de.otto.jobstore.service.api;

import de.otto.jobstore.common.JobRunnable;

import java.util.List;
import java.util.Set;


public interface JobService {

    void registerJob(String name, JobRunnable runnable);

    void executeQueuedJobs() throws Exception;

    void addRunningConstraint(List<String> constraint);

    String queueJob(String name, boolean forceExecution);

    String queueJob(String name);

    void removeQueuedJob(String name);

    void clean();

    Set<String> listJobs();

    void stopAllJobs();
}
