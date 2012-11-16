package de.otto.jobstore.service.impl;

import de.otto.jobstore.common.*;
import de.otto.jobstore.repository.api.JobInfoRepository;
import de.otto.jobstore.service.api.JobService;
import de.otto.jobstore.service.exception.JobNotRegisteredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;

/**
 *
 */
public final class JobServiceImpl implements JobService {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobServiceImpl.class);

    private final Map<String, JobRunnable> jobs = new ConcurrentHashMap<String, JobRunnable>();
    private final Set<Set<String>> runningConstraints = new CopyOnWriteArraySet<Set<String>>();
    private final JobInfoRepository jobInfoRepository;
    private final boolean executionEnabled;

    /**
     * Creates a JobService Object.
     *
     * @param jobInfoRepository The jobInfo Repository to store the jobs in
     */
    public JobServiceImpl(JobInfoRepository jobInfoRepository) {
        this.jobInfoRepository = jobInfoRepository;
        this.executionEnabled = true;
    }

    /**
     * Creates a JobService Object
     *
     * @param jobInfoRepository The jobInfo Repository to store the jobs in
     * @param executionEnabled Flag if jobs will be executed. If set to false no jobs will be started
     */
    public JobServiceImpl(JobInfoRepository jobInfoRepository, boolean executionEnabled) {
        this.jobInfoRepository = jobInfoRepository;
        this.executionEnabled = executionEnabled;
    }

    @Override
    public boolean registerJob(String name, JobRunnable runnable) {
        final boolean inserted;
        if (jobs.containsKey(name)) {
            inserted = false;
        } else {
            jobs.put(name, runnable);
            inserted = true;
        }
        return inserted;
    }

    @Override
    public boolean addRunningConstraint(Set<String> constraint) throws JobNotRegisteredException {
        for (String name : constraint) {
            checkJobName(name);
        }
        return runningConstraints.add(constraint);
    }

    public String executeJob(String name) throws JobNotRegisteredException {
        return executeJob(name, false);
    }

    public String executeJob(String name, boolean forceExecution) throws JobNotRegisteredException {
        final JobRunnable runnable = jobs.get(checkJobName(name));
        String id = null;
        if (jobInfoRepository.hasRunningJob(name) || violatesRunningConstraints(name)) {
            id = jobInfoRepository.create(name, runnable.getMaxExecutionTime(), RunningState.QUEUED, true);
            if (id != null) {
                LOGGER.info("ltag=JobService.executeJob.queuedJob jobInfoName={} jobId={}", name, id);
            }
        } else {
            if (forceExecution || runnable.isExecutionNecessary()) {
                id = jobInfoRepository.create(name, runnable.getMaxExecutionTime(), RunningState.RUNNING, true);
                if (id != null) {
                    LOGGER.info("ltag=JobService.executeJob.executedJob jobInfoName={} jobId={}", name, id);
                    executeJob(name, runnable);
                }
            }
        }
        if (id == null) {
            LOGGER.info("ltag=JobService.executeJob.noJobQueuedOrExecuted jobInfoName={}", name);
        }
        return id;
    }

    @Override
    public void executeQueuedJobs() {
        if (executionEnabled) {
            LOGGER.info("ltag=JobServiceImpl.executeQueuedJobs");
            for (JobInfo jobInfo : jobInfoRepository.findQueuedJobsAscByStartTime()) {
                executeQueuedJob(jobInfo);
            }
        }
    }

    @Override
    public String queueJob(String name) throws JobNotRegisteredException {
        return queueJob(name, false);
    }

    @Override
    public String queueJob(String name, boolean forceExecution) {
        final JobRunnable runnable = jobs.get(checkJobName(name));
        return jobInfoRepository.create(name, runnable.getMaxExecutionTime(), RunningState.QUEUED, forceExecution);
    }

    @Override
    public boolean removeQueuedJob(String name) {
        return jobInfoRepository.removeQueuedJob(name);
    }

    @Override
    public void shutdownJobs() {
        for (String name : jobs.keySet()) {
            final JobInfo runningJob = jobInfoRepository.findRunningByName(name);
            if (runningJob != null && runningJob.getHost().equals(InternetUtils.getHostName())) {
                LOGGER.info("ltag=JobService.shutdownJobs jobInfoName={}", name);
                jobInfoRepository.markAsFinished(name, ResultState.ERROR, "shutdownJobs called from executing host");
            }
        }
    }

    @Override
    public void clean() {
        jobs.clear();
        runningConstraints.clear();
    }

    @Override
    public Set<String> jobNames() {
        return jobs.keySet();
    }

    private void executeJob(String name, JobRunnable runnable) {
        Executors.newSingleThreadExecutor().execute(new JobExecutionRunnable(name, runnable));
    }

    private void executeQueuedJob(final JobInfo jobInfo) {
        final String name = jobInfo.getName();
        final JobRunnable runnable = jobs.get(name);
        if (violatesRunningConstraints(name)) {
            LOGGER.debug("ltag=JobService.executeQueuedJob.violatesRunningConstraints jobInfoName={}", name);
        } else {
            if (jobInfo.isForceExecution() || runnable.isExecutionNecessary()) {
                if (jobInfoRepository.activateQueuedJob(name)) {
                    jobInfoRepository.updateHostThreadInformation(name, InternetUtils.getHostName(), Thread.currentThread().getName());
                    LOGGER.debug("ltag=JobService.executeQueuedJob.activatedQueuedJob jobInfoName={}", name);
                    executeJob(name, runnable);
                } else {
                    LOGGER.warn("ltag=JobService.executeQueuedJob.jobDoesNotExistAnyMore");
                }
            } else {
                if (jobInfoRepository.removeQueuedJob(name)) {
                    LOGGER.warn("ltag=JobService.executeQueuedJob.removeQueuedJob.removedQueuedJob");
                }
            }
        }
    }

    private String checkJobName(String name) throws JobNotRegisteredException {
        if (jobs.containsKey(name)) {
            return name;
        } else {
            throw new JobNotRegisteredException("job with name " + name + " is not registered with this jobService instance");
        }
    }

    private boolean violatesRunningConstraints(String name) {
        for (Set<String> constraint : runningConstraints) {
            if (constraint.contains(name)) {
                for (String constraintJobName : constraint) {
                    if (jobInfoRepository.hasRunningJob(constraintJobName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private class JobExecutionRunnable implements Runnable {
        final String jobName;
        final JobRunnable jobRunnable;

        JobExecutionRunnable(String jobName, JobRunnable jobRunnable) {
            this.jobName = jobName;
            this.jobRunnable = jobRunnable;
        }

        @Override
        public void run() {
            try {
                LOGGER.info("ltag=JobService.JobExecutionRunnable.run jobInfoName={}", jobName);
                jobRunnable.execute(new SimpleJobLogger(jobName, jobInfoRepository));
                jobInfoRepository.markAsFinishedSuccessfully(jobName);
            } catch (Exception ex) {
                jobInfoRepository.markAsFinishedWithException(jobName, ex);
            }
        }
    }

}
