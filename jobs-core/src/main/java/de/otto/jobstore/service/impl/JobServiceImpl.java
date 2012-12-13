package de.otto.jobstore.service.impl;

import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.repository.api.JobInfoRepository;
import de.otto.jobstore.service.api.JobService;
import de.otto.jobstore.service.api.RemoteJobExecutorService;
import de.otto.jobstore.service.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
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

    private final Map<String, JobRunnable> jobs = new ConcurrentHashMap<>();
    private final Set<Set<String>> runningConstraints = new CopyOnWriteArraySet<>();
    private final JobInfoRepository jobInfoRepository;
    private final RemoteJobExecutorService remoteJobExecutorService;
    private boolean executionEnabled = true;

    /**
     * Creates a JobService Object.
     *
     * @param jobInfoRepository The jobInfo Repository to store the jobs in
     * @param remoteJobExecutorService
     */
    public JobServiceImpl(final JobInfoRepository jobInfoRepository, RemoteJobExecutorService remoteJobExecutorService) {
        this.jobInfoRepository = jobInfoRepository;
        this.remoteJobExecutorService = remoteJobExecutorService;
    }

    public boolean isExecutionEnabled() {
        return executionEnabled;
    }

    /**
     * Disables or enables Job execution. Default value is true.
     *
     * @param executionEnabled true - Jobs will be executed<br/>
     *                         false - No jobs will be executed
     */
    public void setExecutionEnabled(boolean executionEnabled) {
        this.executionEnabled = executionEnabled;
    }

    @Override
    public boolean registerJob(final JobRunnable runnable) {
        final boolean inserted;
        final String name = runnable.getName();
        if (jobs.containsKey(name)) {
            inserted = false;
        } else {
            jobs.put(name, runnable);
            inserted = true;
        }
        return inserted;
    }

    @Override
    public boolean addRunningConstraint(final Set<String> constraint) throws JobNotRegisteredException {
        for (String name : constraint) {
            checkJobName(name);
        }
        return runningConstraints.add(Collections.unmodifiableSet(constraint));
    }

    @Override
    public String executeJob(final String name) throws JobNotRegisteredException, JobAlreadyQueuedException,
            JobAlreadyRunningException, JobExecutionNotNecessaryException, JobExecutionDisabledException {
        return executeJob(name, false);
    }

    @Override
    public boolean removeJobFromQueue(String name) {
        return jobInfoRepository.markQueuedAsNotExecuted(name);
    }

    @Override
    public String executeJob(final String name, final boolean forceExecution) throws JobNotRegisteredException,
            JobAlreadyQueuedException, JobAlreadyRunningException, JobExecutionNotNecessaryException,
            JobExecutionDisabledException {
        final JobRunnable runnable = jobs.get(checkJobName(name));
        final String id;
        if (!executionEnabled) {
            throw new JobExecutionDisabledException("Execution of jobs has been disabled");
        } else if (jobInfoRepository.hasJob(name, RunningState.QUEUED.name())) {
            throw new JobAlreadyQueuedException("A job with name " + name + " is already queued for execution");
        } else if (jobInfoRepository.hasJob(name, RunningState.RUNNING.name())) {
            // TODO: bricht mit vorheriger JobInfoRepositorySemantik?
            id = queueJob(runnable, forceExecution, "A job with name " + name + " is already running and queued for execution");
            //throw new JobAlreadyRunningException("A job with name " + name + " is already running and queued for execution");
        } else if (violatesRunningConstraints(name)) {
            // TODO: executeJob semantisch ueberladen, verhaelt sich eher wie ein executeOrAlternativelyQueueJobSometimes
            id = queueJob(runnable, forceExecution, "The job " + name + " violates running constraints and is already queued for execution");
        } else if (forceExecution || runnable.isExecutionNecessary()) {
            id = runJob(runnable, forceExecution, "A job with name " + name + " is already running");
            LOGGER.debug("ltag=JobService.runJob.executingJob jobInfoName={}", name);
            executeJob(runnable);
        } else {
            throw new JobExecutionNotNecessaryException("Execution of job " + name + " was not necessary");
        }
        return id;
    }

    @Override
    public void executeQueuedJobs() {
        if (executionEnabled) {
            LOGGER.info("ltag=JobServiceImpl.executeQueuedJobs");
            for (JobInfo jobInfo : jobInfoRepository.findQueuedJobsSortedAscByCreationTime()) {
                executeQueuedJob(jobInfo);
            }
        }
    }

    @Override
    public void pollRemoteJobs() {
        if (executionEnabled) {
            for (Map.Entry<String, JobRunnable> job : jobs.entrySet()) {
                if (job.getValue().isRemote()) {
                    final JobInfo runningJob = jobInfoRepository.findByNameAndRunningState(job.getKey(), RunningState.RUNNING.name());
                    if (runningJob != null) {
                        if (jobRequiresUpdate(runningJob.getLastModifiedTime(), System.currentTimeMillis(), job.getValue().getPollingInterval())) {
                            final RemoteJobStatus remoteJobStatus = remoteJobExecutorService.getStatus(
                                    URI.create(runningJob.getAdditionalData().get(JobInfoProperty.REMOTE_JOB_URI.val())));
                            if (remoteJobStatus != null) {
                                updateJobStatus(runningJob, remoteJobStatus);
                            }
                        }
                    }
                }
            }
        }
    }

    @PreDestroy
    @Override
    public void shutdownJobs() {
        if (executionEnabled) {
            for (Map.Entry<String, JobRunnable> job : jobs.entrySet()) {
                if (!job.getValue().isRemote()) {
                    final JobInfo runningJob = jobInfoRepository.findByNameAndRunningState(job.getKey(), RunningState.RUNNING.name());
                    if (runningJob != null && runningJob.getHost().equals(InternetUtils.getHostName())) {
                        LOGGER.info("ltag=JobService.shutdownJobs jobInfoName={}", job.getKey());
                        jobInfoRepository.markRunningAsFinished(job.getKey(), ResultState.FAILED, "shutdownJobs called from executing host");
                    }
                }
            }
        }
    }

    @Override
    public void clean() {
        jobs.clear();
        runningConstraints.clear();
    }

    @Override
    public Set<String> listJobNames() {
        return Collections.unmodifiableSet(jobs.keySet());
    }

    @Override
    public Set<Set<String>> listRunningConstraints() {
        return Collections.unmodifiableSet(runningConstraints);
    }

    private boolean jobRequiresUpdate(Date lastModificationTime, long currentTime, long pollingInterval) {
        return new Date(currentTime - pollingInterval).after(lastModificationTime);
    }

    private void updateJobStatus(JobInfo jobInfo, RemoteJobStatus remoteJobStatus) {
        if (remoteJobStatus.status == RemoteJobStatus.Status.RUNNING) {
            jobInfoRepository.setLogLines(jobInfo.getName(), remoteJobStatus.logLines);
        } else if (remoteJobStatus.status == RemoteJobStatus.Status.FINISHED) {
            final RemoteJobResult result = remoteJobStatus.result;
            if (result.ok) {
                jobInfoRepository.markRunningAsFinishedSuccessfully(jobInfo.getName());
            } else {
                jobInfoRepository.addAdditionalData(jobInfo.getName(), "exitCode", String.valueOf(result.exitCode));
                jobInfoRepository.markRunningAsFinished(jobInfo.getName(), ResultState.FAILED, result.message);
            }
        }
    }

    private void executeJob(JobRunnable runnable) {
        Executors.newSingleThreadExecutor().execute(new JobExecutionRunnable(runnable, jobInfoRepository));
    }

    private void executeQueuedJob(final JobInfo jobInfo) {
        final String name = jobInfo.getName();
        final String id = jobInfo.getId();
        final JobRunnable runnable = jobs.get(name);
        if (jobInfoRepository.hasJob(name, RunningState.RUNNING.name())) {
            LOGGER.info("ltag=JobService.executeQueuedJob.alreadyRunning jobInfoName={} jobInfoId={}", name, id);
        } else if (violatesRunningConstraints(name)) {
            LOGGER.info("ltag=JobService.executeQueuedJob.violatesRunningConstraints jobInfoName={} jobInfoId={}", name, id);
        } else if (jobInfo.isForceExecution() || runnable.isExecutionNecessary()) {
            activateQueuedJob(name, id, runnable);
        } else if (jobInfoRepository.markAsFinishedById(jobInfo.getId(), ResultState.NOT_EXECUTED)) {
            LOGGER.warn("ltag=JobService.executeQueuedJob.executionIsNotNecessary jobInfoName={} jobInfoId={}", name, id);
        }
    }

    private String queueJob(JobRunnable runnable, boolean forceExecution, String exceptionMessage)
            throws JobAlreadyQueuedException{
        final String id = jobInfoRepository.create(runnable.getName(), runnable.getMaxExecutionTime(),
                RunningState.QUEUED, forceExecution, runnable.isRemote(), null);
        if (id == null) {
            throw new JobAlreadyQueuedException(exceptionMessage);
        }
        return id;
    }

    private String runJob(JobRunnable runnable, boolean forceExecution, String exceptionMessage)
            throws JobAlreadyRunningException {
        final String id = jobInfoRepository.create(runnable.getName(), runnable.getMaxExecutionTime(),
                RunningState.RUNNING, forceExecution, runnable.isRemote(), null);
        if (id == null) {
            throw new JobAlreadyRunningException(exceptionMessage);
        }
        return id;
    }

    private void activateQueuedJob(String name, String id, JobRunnable runnable) {
        if (jobInfoRepository.activateQueuedJob(name)) {
            jobInfoRepository.updateHostThreadInformation(name);
            LOGGER.info("ltag=JobService.activateQueuedJob.activate jobInfoName={} jobInfoId={}", name, id);
            executeJob(runnable);
        } else {
            LOGGER.warn("ltag=JobService.activateQueuedJob.jobIsNotQueuedAnyMore jobInfoName={} jobInfoId={}", name, id);
        }
    }

    private String checkJobName(final String name) throws JobNotRegisteredException {
        if (jobs.containsKey(name)) {
            return name;
        } else {
            throw new JobNotRegisteredException("job with name " + name + " is not registered with this jobService instance");
        }
    }

    private boolean violatesRunningConstraints(final String name) {
        for (Set<String> constraint : runningConstraints) {
            if (constraint.contains(name)) {
                for (String constraintJobName : constraint) {
                    if (jobInfoRepository.hasJob(constraintJobName, RunningState.RUNNING.name())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

}
