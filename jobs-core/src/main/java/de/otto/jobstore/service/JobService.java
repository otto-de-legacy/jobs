package de.otto.jobstore.service;

import de.otto.jobstore.common.*;
import de.otto.jobstore.repository.JobInfoRepository;
import de.otto.jobstore.service.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;

/**
 *  This service allows to handle multiple jobs and their associated runnables. A job has to be registered before it
 *  can be executed or cued. The service allows only one queued and one running job for each distinct job name.
 *
 *  In order to execute jobs they have to be queued and afterwards executed by callings {#executeQueuedJobs}. By adding
 *  running constraints it is possible to define jobs that are not allowed to run at the same time.
 */
public class JobService {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobService.class);

    private final Map<String, JobRunnable> jobs = new ConcurrentHashMap<>();
    private final Set<Set<String>> runningConstraints = new CopyOnWriteArraySet<>();
    private final JobInfoRepository jobInfoRepository;
    private boolean executionEnabled = true;

    /**
     * Creates a JobService Object.
     *
     * @param jobInfoRepository The jobInfo Repository to store the jobs in
     */
    public JobService(final JobInfoRepository jobInfoRepository) {
        this.jobInfoRepository = jobInfoRepository;
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

    /**
     * Registers a job with the given runnable in this job service
     *
     * @param runnable The job runnable
     * @return true - The job was successfully registered<br>
     *     false - A job with the given name is already registered
     */
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

    /**
     * Adds a running constraint to this JobService instance.
     *
     * @param constraint The names of the jobs that are not allowed to run at the same time
     * @return true - If the running constraint was successfully added<br>
     *     false - If the running constraint already exists
     * @throws de.otto.jobstore.service.exception.JobNotRegisteredException Thrown if the constraint contains a name of
     * a job which is not registered with this JobService instance
     */
    public boolean addRunningConstraint(final Set<String> constraint) throws JobNotRegisteredException {
        for (String name : constraint) {
            checkJobName(name);
        }
        return runningConstraints.add(Collections.unmodifiableSet(constraint));
    }

    /**
     * Removes a job with the given from the queue and sets its result state to not executed.
     *
     * @param name The name of the job
     * @return true - If a queued job with the given name was found</br>
     *         false - If no queued job with the given name could be found
     *
     */
    public boolean removeJobFromQueue(String name) {
        return jobInfoRepository.markQueuedAsNotExecuted(name);
    }

    /**
     * Executes a job with the given name and returns its ID. If a job is already running or running it would violate
     * running constraints it this job will be added to the queue. If a job is already queued an exception will be thrown.
     *
     * @param name The name of the job to execute
     * @return The id of the executing or queued job
     * @throws JobNotRegisteredException Thrown if no job with the given name was registered with this JobService instance
     * @throws JobAlreadyQueuedException If a job with the given name is already queued for execution or another
     * JobService instance queued the job while this method was executed
     * @throws JobAlreadyRunningException If another JobService instance executed a job with the given name while this
     * method was executed
     * @throws JobExecutionNotNecessaryException If the execution of the job was not necessary
     * @throws JobExecutionDisabledException If job execution has been disabled
     */
    public String executeJob(final String name) throws JobNotRegisteredException, JobAlreadyQueuedException,
            JobAlreadyRunningException, JobExecutionNotNecessaryException, JobExecutionDisabledException {
        return executeJob(name, JobExecutionPriority.CHECK_PRECONDITIONS);
    }

    /**
     * Executes a job with the given name and returns its ID. If a job is already running or running it would violate
     * running constraints it this job will be added to the queue. If a job is already queued an exception will be thrown.
     *
     * @param name The name of the job to execute
     * @param executionPriority The priority with which the job is to be executed
     * @return The id of the executing or queued job
     * @throws JobNotRegisteredException Thrown if no job with the given name was registered with this JobService instance
     * @throws JobAlreadyQueuedException If a job with the given name is already queued for execution or another
     * JobService instance queued the job while this method was executed
     * @throws JobAlreadyRunningException If another JobService instance executed a job with the given name while this
     * method was executed
     * @throws JobExecutionNotNecessaryException If the execution of the job was not necessary
     * @throws JobExecutionDisabledException If job execution has been disabled
     */
    public String executeJob(final String name, final JobExecutionPriority executionPriority) throws JobNotRegisteredException,
            JobAlreadyQueuedException, JobAlreadyRunningException, JobExecutionNotNecessaryException,
            JobExecutionDisabledException {
        final JobRunnable runnable = jobs.get(checkJobName(name));
        final String id;
        if (!executionEnabled) {
            throw new JobExecutionDisabledException("Execution of jobs has been disabled");
        }
        final JobInfo queuedJobInfo = jobInfoRepository.findByNameAndRunningState(name, RunningState.QUEUED);
        if (queuedJobInfo != null) {
            if (queuedJobInfo.getExecutionPriority().hasLowerPriority(executionPriority)) {
                jobInfoRepository.remove(queuedJobInfo.getId());
                id = queueJob(runnable, executionPriority, "A job with name " + name + " is already running and queued for execution");
            } else {
                throw new JobAlreadyQueuedException("A job with name " + name + " is already queued for execution");
            }
        } else {
            final JobInfo runningJobInfo = jobInfoRepository.findByNameAndRunningState(name, RunningState.RUNNING);
            if (runningJobInfo == null) {
                id = queueJob(runnable, executionPriority, "A job with name " + name + " is already running and queued for execution");
                LOGGER.debug("ltag=JobService.createJob.executingJob jobInfoName={}", name);
                executeQueuedJob(id, name, executionPriority);
            } else if (runningJobInfo.getExecutionPriority().hasLowerPriority(executionPriority)) {
                id = queueJob(runnable, executionPriority, "A job with name " + name + " is already running and queued for execution");
            } else {
                throw new JobExecutionNotNecessaryException("Execution of job " + name + " was not necessary");
            }
        }
        return id;
    }

    /**
     * Executes all queued jobs registered with this JobService instance asynchronously in the order they were queued.
     */
    public void executeQueuedJobs() {
        if (executionEnabled) {
            LOGGER.info("ltag=JobServiceImpl.executeQueuedJobs");
            for (JobInfo jobInfo : jobInfoRepository.findQueuedJobsSortedAscByCreationTime()) {
                executeQueuedJob(jobInfo.getId(), jobInfo.getName(), jobInfo.getExecutionPriority());
            }
        }
    }

    /**
     * Polls all remote jobs and updates their status if necessary
     */
    public void pollRemoteJobs() {
        if (executionEnabled) {
            for (Map.Entry<String, JobRunnable> job : jobs.entrySet()) {
                JobRunnable jobRunnable = job.getValue();
                if (jobRunnable.isRemote()) {
                    final JobInfo runningJob = jobInfoRepository.findByNameAndRunningState(job.getKey(), RunningState.RUNNING);
                    if (runningJob != null) {
                        if (jobRequiresUpdate(runningJob.getLastModifiedTime(), System.currentTimeMillis(), jobRunnable.getPollingInterval())) {
                            final RemoteJobStatus remoteJobStatus = jobRunnable.getRemoteStatus(createJobExecutionContext(jobRunnable.getName(), runningJob.getId(), runningJob.getExecutionPriority()));
                            if (remoteJobStatus != null) {
                                updateJobStatus(runningJob, remoteJobStatus, job.getValue());
                            }
                        }
                    } else {
                        LOGGER.info("No running instance of jobName={} found, skip status check", job.getKey());
                    }
                }
            } // for all jobs
        }
    }

    /**
     * Stops all jobs registered with this JobService and running on this host.
     */
    @PreDestroy
    public void shutdownJobs() {
        if (executionEnabled) {
            for (Map.Entry<String, JobRunnable> job : jobs.entrySet()) {
                if (!job.getValue().isRemote()) {
                    final JobInfo runningJob = jobInfoRepository.findByNameAndRunningState(job.getKey(), RunningState.RUNNING);
                    if (runningJob != null && runningJob.getHost().equals(InternetUtils.getHostName())) {
                        LOGGER.info("ltag=JobService.shutdownJobs jobInfoName={}", job.getKey());
                        jobInfoRepository.markRunningAsFinished(job.getKey(), ResultCode.FAILED, "shutdownJobs called from executing host");
                    }
                }
            }
        }
    }

    /**
     * Removed all registered jobs and constraints from the JobService instance
     */
    public void clean() {
        jobs.clear();
        runningConstraints.clear();
    }

    /**
     * Returns the Names of all registered jobs
     */
    public Collection<String> listJobNames() {
        final List<String> jobNames = new ArrayList<>(jobs.keySet());
        Collections.sort(jobNames);
        return Collections.unmodifiableList(jobNames);
    }

    /**
     * Returns the Set of all constraints
     */
    public Set<Set<String>> listRunningConstraints() {
        return Collections.unmodifiableSet(runningConstraints);
    }

    private boolean jobRequiresUpdate(Date lastModificationTime, long currentTime, long pollingInterval) {
        return new Date(currentTime - pollingInterval).after(lastModificationTime);
    }

    private void updateJobStatus(JobInfo jobInfo, RemoteJobStatus remoteJobStatus, JobRunnable jobRunnable) {
        LOGGER.info("ltag=JobService.updateJobStatus jobName={} jobId={} status={}", new Object[]{jobInfo.getName(), jobInfo.getId(), remoteJobStatus.status});
        jobInfoRepository.setLogLines(jobInfo.getName(), remoteJobStatus.logLines);
        if (remoteJobStatus.message != null && remoteJobStatus.message.length() > 0) {
            jobInfoRepository.setStatusMessage(jobInfo.getName(), remoteJobStatus.message);
        }
        if (remoteJobStatus.status == RemoteJobStatus.Status.FINISHED) {
            final JobExecutionContext context = createJobExecutionContext(jobInfo.getName(), jobInfo.getId(), jobInfo.getExecutionPriority(), remoteJobStatus.logLines);
            context.setResultCode(remoteJobStatus.result.ok ? ResultCode.SUCCESSFUL : ResultCode.FAILED);
            context.setResultMessage(remoteJobStatus.message);
            if (remoteJobStatus.result.ok) {
                try {
                    jobRunnable.afterExecution(context);
                    jobInfoRepository.markRunningAsFinished(jobRunnable.getName(), context.getResultCode(), remoteJobStatus.result.message);
                } catch (Exception e) {
                    LOGGER.error("ltag=JobService.updateJobStatus.afterExecution jobName=" + jobRunnable.getName() + " jobId=" + jobInfo.getId() + " failed: " + e.getMessage(), e);
                    jobInfoRepository.markRunningAsFinishedWithException(jobRunnable.getName(), e);
                }
            } else {
                LOGGER.warn("ltag=JobService.updateJobStatus.resultNotOk jobName={} jobId={} exitCode={} message={}",
                        new Object[]{jobInfo.getName(), jobInfo.getId(), remoteJobStatus.result.exitCode, remoteJobStatus.result.message});
                jobInfoRepository.addAdditionalData(jobInfo.getName(), "exitCode", String.valueOf(remoteJobStatus.result.exitCode));
                jobInfoRepository.markRunningAsFinished(jobInfo.getName(), ResultCode.FAILED, remoteJobStatus.result.message);
            }
        }
    }

    private void executeJob(String id, JobRunnable runnable, JobExecutionPriority executionPriority) {
        Executors.newSingleThreadExecutor().execute(new JobExecutionRunnable(
                runnable, jobInfoRepository, createJobExecutionContext(runnable.getName(), id, executionPriority)));
    }

    private JobExecutionContext createJobExecutionContext(String jobName, String jobId, JobExecutionPriority priority) {
        final JobLogger jobLogger = new SimpleJobLogger(jobName, jobInfoRepository);
        return new JobExecutionContext(jobId, jobLogger, priority);
    }

    private JobExecutionContext createJobExecutionContext(String jobName, String jobId, JobExecutionPriority priority, List<String> logLines) {
        final JobLogger jobLogger = new SimpleJobLogger(jobName, jobInfoRepository, logLines);
        return new JobExecutionContext(jobId, jobLogger, priority);
    }

    private void executeQueuedJob(String id, final String name, JobExecutionPriority executionPriority) {
        final JobRunnable runnable = jobs.get(name);
        if (jobInfoRepository.hasJob(name, RunningState.RUNNING)) {
            LOGGER.info("ltag=JobService.executeQueuedJob.alreadyRunning jobInfoName={} jobInfoId={}", name, id);
        } else if (violatesRunningConstraints(name)) {
            LOGGER.info("ltag=JobService.executeQueuedJob.violatesRunningConstraints jobInfoName={} jobInfoId={}", name, id);
        } else {
            activateQueuedJob(name, id, executionPriority, runnable);
        }
    }

    private String queueJob(JobRunnable runnable, JobExecutionPriority jobExecutionPriority, String exceptionMessage)
            throws JobAlreadyQueuedException{
        final String id = jobInfoRepository.create(runnable.getName(), runnable.getMaxExecutionTime(),
                RunningState.QUEUED, jobExecutionPriority, runnable.getParameters(), null);
        if (id == null) {
            throw new JobAlreadyQueuedException(exceptionMessage);
        }
        return id;
    }

    private void activateQueuedJob(String name, String id, JobExecutionPriority executionPriority, JobRunnable runnable) {
        if (jobInfoRepository.activateQueuedJob(name)) {
            jobInfoRepository.updateHostThreadInformation(name);
            LOGGER.info("ltag=JobService.activateQueuedJob.activate jobInfoName={} jobInfoId={}", name, id);
            executeJob(id, runnable, executionPriority);
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
                    if (jobInfoRepository.hasJob(constraintJobName, RunningState.RUNNING)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

}
