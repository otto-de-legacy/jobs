package de.otto.jobstore.service;

import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.common.util.InternetUtils;
import de.otto.jobstore.repository.JobDefinitionRepository;
import de.otto.jobstore.repository.JobInfoRepository;
import de.otto.jobstore.service.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;

/**
 * This service allows to handle multiple jobs and their associated runnables. A job has to be registered before it
 * can be executed or cued. The service allows only one queued and one running job for each distinct job name.
 * <p/>
 * In order to execute jobs they have to be queued and afterwards executed by callings {#executeQueuedJobs}. By adding
 * running constraints it is possible to define jobs that are not allowed to run at the same time.
 */
public class JobService {

    private static final long JOB_INFO_CACHE_UPDATE_INTERVAL = 10000;
    private static final Logger LOGGER = LoggerFactory.getLogger(JobService.class);

    static final Map<String, String> NO_PARAMETERS = Collections.emptyMap();

    private final Map<String, JobRunnable> jobs = new ConcurrentHashMap<>();
    private final Set<Set<String>> runningConstraints = new CopyOnWriteArraySet<>();
    private JobDefinitionRepository jobDefinitionRepository;
    private JobInfoRepository jobInfoRepository;
    private ActiveChecker activeChecker;

    protected int awaitTerminationSeconds = 30;
    protected boolean desynchronize = true;

    private volatile boolean shutdown = false;

    /**
     * Creates a JobService Object.
     *
     * @param jobDefinitionRepository The jobDefinition repository to store definitions in
     * @param jobInfoRepository       The jobInfo Repository to store the jobs in
     * @param activeChecker           The activeChecker to determine if this jobService is active or not
     */
    public JobService(JobDefinitionRepository jobDefinitionRepository, final JobInfoRepository jobInfoRepository, ActiveChecker activeChecker) {
        this.jobDefinitionRepository = jobDefinitionRepository;
        this.jobInfoRepository = jobInfoRepository;
        this.activeChecker = activeChecker;
        this.jobDefinitionRepository.addOrUpdate(StoredJobDefinition.JOB_EXEC_SEMAPHORE);
    }

    /**
     * Creates a JobService Object.
     *
     * @param jobDefinitionRepository The jobDefinition repository to store definitions in
     * @param jobInfoRepository       The jobInfo Repository to store the jobs in
     */
    public JobService(JobDefinitionRepository jobDefinitionRepository, JobInfoRepository jobInfoRepository) {
        this(jobDefinitionRepository, jobInfoRepository, new ActiveChecker() {
            @Override
            public boolean isActive() {
                return true;
            }
        });
    }

    public boolean isExecutionEnabled() {
        return isJobEnabled(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName());
    }

    /**
     * Disables or enables Job execution. Default value is true.
     *
     * @param executionEnabled true - Jobs will be executed<br/>
     *                         false - No jobs will be executed
     */
    public void setExecutionEnabled(boolean executionEnabled) {
        jobDefinitionRepository.setJobExecutionEnabled(StoredJobDefinition.JOB_EXEC_SEMAPHORE.getName(), executionEnabled);
    }

    /**
     * Registers a job with the given runnable in this job service
     *
     * @param jobRunnable The jobRunnable
     * @return true - The job was successfully registered<br>
     * false - A job with the given name is already registered
     */
    public boolean registerJob(final JobRunnable jobRunnable) {
        return registerJob(jobRunnable, false);
    }

    /**
     * Registers a job with the given runnable in this job service
     *
     * @param jobRunnable The jobRunnable
     * @return true - The job was successfully registered<br>
     * false - A job with the given name is already registered
     */
    protected boolean registerJob(final JobRunnable jobRunnable, boolean reregister) {
        final JobDefinition jobDefinition = jobRunnable.getJobDefinition();
        final String name = jobDefinition.getName();
        if (!reregister && isJobRegistered(name)) {
            LOGGER.warn("ltag=JobService.createJob.registerJob Tried to re-register job with name={}", name);
            return false;
        } else {
            jobs.put(name, jobRunnable);
            jobDefinitionRepository.addOrUpdate(new StoredJobDefinition(jobDefinition));
            return true;
        }
    }

    private boolean isJobRegistered(String name) {
        return jobs.containsKey(name);
    }

    /**
     * Check if execution of a job is enabled
     *
     * @param name The name of the job to check
     * @return true - The execution of the job is enabled<br>
     * false - The execution of the job is disabled
     * @throws JobNotRegisteredException If the job is not registered with this jobService instance
     */
    public boolean isJobExecutionEnabled(final String name) throws JobNotRegisteredException {
        checkIfJobIsRegistered(name);
        final StoredJobDefinition jobDefinition = jobDefinitionRepository.find(name);
        return !jobDefinition.isDisabled();
    }

    /**
     * Disable or enable the execution of a job
     *
     * @param name             The name of the job
     * @param executionEnabled true - The execution of the job is enabled<br>
     *                         false - The execution of the job is disabled
     * @throws JobNotRegisteredException If the job is not registered with this jobService instance
     */
    public void setJobExecutionEnabled(String name, boolean executionEnabled) throws JobNotRegisteredException {
        checkIfJobIsRegistered(name);
        jobDefinitionRepository.setJobExecutionEnabled(name, executionEnabled);
    }

    /**
     * Adds a running constraint to this JobService instance.
     *
     * @param constraint The names of the jobs that are not allowed to run at the same time
     * @return true - If the running constraint was successfully added<br>
     * false - If the running constraint already exists
     * @throws de.otto.jobstore.service.exception.JobNotRegisteredException Thrown if the constraint contains a name of
     *                                                                      a job which is not registered with this JobService instance
     *
     * deprecated, use addRunningConstraintWithoutChecks instead. There is no reason to check for non existing jobs, this only enforces a strange order
     */
    @Deprecated
    public boolean addRunningConstraint(final Set<String> constraint) throws JobNotRegisteredException {
        for (String name : constraint) {
            checkIfJobIsRegistered(name);
        }
        return addRunningConstraintWithoutChecks(constraint);
    }

    /**
     * Adds a running constraint to this JobService instance.
     *
     * @param constraint The names of the jobs that are not allowed to run at the same time
     * @return true - If the running constraint was successfully added<br>
     * false - If the running constraint already exists
     *
     */
    public boolean addRunningConstraintWithoutChecks(final Set<String> constraint) {
        return runningConstraints.add(Collections.unmodifiableSet(constraint));
    }

    /**
     * Executes a job with the given name and returns its ID. If a job is already running or running it would violate
     * running constraints it this job will be added to the queue. If a job is already queued an exception will be thrown.
     *
     * @param name The name of the job to execute
     * @return The id of the executing or queued job
     * @throws JobNotRegisteredException         Thrown if no job with the given name was registered with this JobService instance
     * @throws JobAlreadyQueuedException         If a job with the given name is already queued for execution or another
     *                                           JobService instance queued the job while this method was executed
     * @throws JobAlreadyRunningException        If another JobService instance executed a job with the given name while this
     *                                           method was executed
     * @throws JobExecutionNotNecessaryException If the execution of the job was not necessary
     * @throws JobExecutionDisabledException     If job execution has been disabled
     */
    public String executeJob(final String name) throws JobNotRegisteredException, JobAlreadyQueuedException,
            JobAlreadyRunningException, JobExecutionNotNecessaryException, JobExecutionDisabledException, JobServiceNotActiveException {
        return executeJob(name, NO_PARAMETERS);
    }

    /**
     * Executes a job with the given name and returns its ID. If a job is already running or running it would violate
     * running constraints it this job will be added to the queue. If a job is already queued an exception will be thrown.
     *
     * @param name       The name of the job to execute
     * @param parameters parameters to use
     * @return The id of the executing or queued job
     * @throws java.lang.NullPointerException    if parameters are null
     * @throws JobNotRegisteredException         Thrown if no job with the given name was registered with this JobService instance
     * @throws JobAlreadyQueuedException         If a job with the given name is already queued for execution or another
     *                                           JobService instance queued the job while this method was executed
     * @throws JobAlreadyRunningException        If another JobService instance executed a job with the given name while this
     *                                           method was executed
     * @throws JobExecutionNotNecessaryException If the execution of the job was not necessary
     * @throws JobExecutionDisabledException     If job execution has been disabled
     */
    public String executeJob(final String name, Map<String, String> parameters) throws JobNotRegisteredException, JobAlreadyQueuedException,
            JobAlreadyRunningException, JobExecutionNotNecessaryException, JobExecutionDisabledException, JobServiceNotActiveException {
        return executeJob(name, JobExecutionPriority.CHECK_PRECONDITIONS, parameters);
    }

    /**
     * Executes a job with the given name and returns its ID. If a job is already running or running it would violate
     * running constraints it this job will be added to the queue. If a job is already queued an exception will be thrown.
     *
     * @param name              The name of the job to execute
     * @param executionPriority The priority with which the job is to be executed
     * @return The id of the executing or queued job
     * @throws JobNotRegisteredException         Thrown if no job with the given name was registered with this JobService instance
     * @throws JobAlreadyQueuedException         If a job with the given name is already queued for execution or another
     *                                           JobService instance queued the job while this method was executed
     * @throws JobAlreadyRunningException        If another JobService instance executed a job with the given name while this
     *                                           method was executed
     * @throws JobExecutionNotNecessaryException If the execution of the job was not necessary
     * @throws JobExecutionDisabledException     If job execution has been disabled
     */
    public String executeJob(final String name, final JobExecutionPriority executionPriority) throws JobNotRegisteredException,
            JobAlreadyQueuedException, JobAlreadyRunningException, JobExecutionNotNecessaryException,
            JobExecutionDisabledException, JobServiceNotActiveException {
        return executeJob(name, executionPriority, NO_PARAMETERS);
    }

    /**
     * Executes a job with the given name and returns its ID. If a job is already running or running it would violate
     * running constraints it this job will be added to the queue. If a job is already queued an exception will be thrown.
     *
     * @param name              The name of the job to execute
     * @param executionPriority The priority with which the job is to be executed
     * @param parameters        parameters to use
     * @return The id of the executing or queued job
     * @throws java.lang.NullPointerException    if parameters are null
     * @throws JobNotRegisteredException         Thrown if no job with the given name was registered with this JobService instance
     * @throws JobAlreadyQueuedException         If a job with the given name is already queued for execution or another
     *                                           JobService instance queued the job while this method was executed
     * @throws JobAlreadyRunningException        If another JobService instance executed a job with the given name while this
     *                                           method was executed
     * @throws JobExecutionNotNecessaryException If the execution of the job was not necessary
     * @throws JobExecutionDisabledException     If job execution has been disabled
     */
    public String executeJob(final String name, final JobExecutionPriority executionPriority, Map<String, String> parameters) throws JobNotRegisteredException,
            JobAlreadyQueuedException, JobAlreadyRunningException, JobExecutionNotNecessaryException,
            JobExecutionDisabledException, JobServiceNotActiveException {
        checkParameters(parameters);
        checkIfJobServiceIsActive();
        checkIfJobExecutionIsEnabled();
        checkIfJobIsRegistered(name);
        checkIfJobIsDisabled(name);
        final JobRunnable runnable = jobs.get(name);
        final JobInfo queuedJobInfo = jobInfoRepository.findByNameAndRunningState(name, RunningState.QUEUED);
        if (queuedJobInfo == null) {
            return executeJobIsNecessaryAndPossible(name, executionPriority, runnable, parameters);
        } else {
            return queueJobIfNecessaryAndPossible(name, executionPriority, parameters, runnable, queuedJobInfo);
        }
    }

    private void checkParameters(Map<String, String> parameters) {
        if (parameters == null) {
            throw new NullPointerException("parameters may not be null");
        }
    }

    private String executeJobIsNecessaryAndPossible(String name, JobExecutionPriority executionPriority, JobRunnable runnable, Map<String, String> parameters)
            throws JobAlreadyRunningException, JobAlreadyQueuedException, JobExecutionNotNecessaryException {
        final JobInfo runningJobInfo = jobInfoRepository.findByNameAndRunningState(name, RunningState.RUNNING);
        if (runningJobInfo == null) {
            return executeJobOrQueueIfRunningConstraintsAreViolated(name, executionPriority, parameters, runnable);
        } else if (runningJobInfo.hasLowerPriority(executionPriority)) {
            return queueJob(runnable, executionPriority, parameters, "A job with name " + name + " is already running and queued for execution");
        } else {
            throw new JobExecutionNotNecessaryException("Execution of job " + name + " was not necessary");
        }
    }

    private String executeJobOrQueueIfRunningConstraintsAreViolated(String name, JobExecutionPriority executionPriority, Map<String, String> parameters, JobRunnable runnable)
            throws JobAlreadyRunningException, JobAlreadyQueuedException {
        final String id = runJob(runnable, executionPriority, parameters, "A job with name " + name + " is already running and queued for execution");
        if (violatesRunningConstraints(name, true)) {
            LOGGER.info("ltag=JobService.executeJobIsNecessary.violatesRunningConstraints jobInfoName={} jobInfoId={}", name, id);
            if (!jobInfoRepository.deactivateRunningJob(id)) {
                jobInfoRepository.remove(id);
                throw new JobAlreadyQueuedException("Job could not be deactivated because another job is already queued and was thus deleted");
            }
        } else {
            LOGGER.debug("ltag=JobService.executeJobIsNecessary jobInfoName={}", name);
            executeJob(runnable, id, executionPriority);
        }
        return id;
    }

    private String queueJobIfNecessaryAndPossible(String name, JobExecutionPriority executionPriority, Map<String, String> parameters, JobRunnable runnable, JobInfo queuedJobInfo) throws JobAlreadyQueuedException {
        if (queuedJobInfo.hasLowerPriority(executionPriority)) {
            jobInfoRepository.remove(queuedJobInfo.getId());
            return queueJob(runnable, executionPriority, parameters, "A job with name " + name + " is already running and queued for execution");
        } else {
            throw new JobAlreadyQueuedException("A job with name " + name + " is already queued for execution");
        }
    }

    private void checkIfJobIsDisabled(String name) throws JobNotRegisteredException, JobExecutionDisabledException {
        if (!isJobExecutionEnabled(name)) {
            throw new JobExecutionDisabledException("Execution of jobs with name " + name + " has been disabled");
        }
    }

    private void checkIfJobExecutionIsEnabled() throws JobExecutionDisabledException {
        if (isExecutionDisabled()) {
            throw new JobExecutionDisabledException("Execution of jobs has been disabled");
        }
    }

    private void checkIfJobServiceIsActive() throws JobServiceNotActiveException {
        if (!activeChecker.isActive()) {
            throw new JobServiceNotActiveException("Job Service is not active");
        }
    }


    public void abortJob(String id) {
        jobInfoRepository.abortJob(id);
    }

    /**
     * Executes all queued jobs registered with this JobService instance asynchronously in the order they were queued.
     */
    public void executeQueuedJobs() {
        if (!activeChecker.isActive()) {
            LOGGER.info("ltag=JobService not active");
            return;
        }

        LOGGER.info("ltag=JobService.executeQueuedJobs called");
        try {
            doExecuteQueuedJobs();
        } catch (Exception e) {
            LOGGER.error("ltag=JobService.executeQueuedJobs exception occurred", e);
        }
        LOGGER.info("ltag=JobService.executeQueuedJobs finished");
    }

    private void doExecuteQueuedJobs() {
        if (isExecutionDisabled()) {
            return;
        }
        desynchronize();
        LOGGER.info("ltag=JobService.executeQueuedJobs");
        for (JobInfo jobInfo : jobInfoRepository.findQueuedJobsSortedAscByCreationTime()) {
            try {
                checkIfJobIsDisabled(jobInfo.getName());
            } catch (JobNotRegisteredException e) {
                LOGGER.info("ltag=JobService.executeQueuedJobs.notRegistered jobName={}", jobInfo.getName());
                break;
            } catch (JobExecutionDisabledException e) {
                LOGGER.info("ltag=JobService.executeQueuedJobs.isDisabled jobName={}", jobInfo.getName());
                break;
            }
            if (jobInfoRepository.hasJob(jobInfo.getName(), RunningState.RUNNING)) {
                LOGGER.info("ltag=JobService.executeQueuedJobs.alreadyRunning jobInfoName={}", jobInfo.getName());
                break;
            }
            final JobRunnable runnable = jobs.get(jobInfo.getName());
            executeQueuedJob(runnable, jobInfo.getId(), jobInfo.getExecutionPriority());
        }
    }

    private void desynchronize() {
        if (desynchronize) {
            try {
                // desynchronize with other systems in environment, wait up to 3 seconds
                Thread.sleep(1 + ThreadLocalRandom.current().nextLong(TimeUnit.SECONDS.toMillis(3)));
            } catch (InterruptedException e) {
                // this is ok, we don't need to take care about Interruption here
            }
        }
    }

    /**
     * Polls all remote jobs and updates their status if necessary
     */
    public void pollRemoteJobs() {
        if (!activeChecker.isActive()) {
            LOGGER.info("ltag=JobService not active");
            return;
        }

        LOGGER.info("ltag=JobService.pollRemoteJobs called");
        try {
            doPollRemoteJobs();
        } catch (Exception e) {
            LOGGER.error("ltag=JobService.pollRemoteJobs exception occurred", e);
        }
        LOGGER.info("ltag=JobService.pollRemoteJobs finished");
    }

    private void doPollRemoteJobs() {
        if (isExecutionDisabled()) {
            return;
        }
        desynchronize();
        for (JobRunnable jobRunnable : jobs.values()) {
            if (jobRunnable.getJobDefinition().isRemote()) {
                final JobDefinition definition = jobRunnable.getJobDefinition();
                final JobInfo runningJob = jobInfoRepository.findByNameAndRunningState(definition.getName(), RunningState.RUNNING);
                if (runningJob != null && jobAgedOverInterval(runningJob.getLastModifiedTime(), System.currentTimeMillis(), definition.getPollingInterval()) &&
                        runningJob.getAdditionalData().containsKey(JobInfoProperty.REMOTE_JOB_URI.val())) {
                    final JobRunnable runnable = jobs.get(definition.getName());
                    final RemoteJobStatus remoteJobStatus = runnable.getRemoteStatus(
                            createJobExecutionContext(runningJob.getId(), definition, runningJob.getExecutionPriority(), null));
                    if (remoteJobStatus != null) {
                        updateJobStatus(runningJob, runnable, remoteJobStatus, definition);
                    }
                } else {
                    LOGGER.info("ltag=JobService.pollRemoteJobs jobName={} " + runningJob == null ? "has no running instance." : "is still fresh.", definition.getName());
                }
            }
        }
    }

    @PostConstruct
    public void startup() {
        LOGGER.info("startup called");
        shutdown = false;
    }

    /**
     * Stops all jobs registered with this JobService and running on this host.
     */
    @PreDestroy
    public void shutdownJobs() {
        LOGGER.info("shutdownJobs called");
        if (isExecutionDisabled()) {
            return;
        }
        // abort first
        for (JobRunnable jobRunnable : jobs.values()) {
            if (!jobRunnable.getJobDefinition().isRemote()) {
                final String name = jobRunnable.getJobDefinition().getName();
                final JobInfo runningJob = jobInfoRepository.findByNameAndRunningState(name, RunningState.RUNNING);
                if (runningJob != null && runningJob.getHost().equals(InternetUtils.getHostName())) {
                    LOGGER.info("ltag=JobService.shutdownJobs jobInfoName={}", name);
                    abortJob(runningJob.getId());
                }
            }
        }
        shutdown = true;

        shutdownJobExecutorService(false);

        // mark as aborted if still running
        for (JobRunnable jobRunnable : jobs.values()) {
            if (!jobRunnable.getJobDefinition().isRemote()) {
                final String name = jobRunnable.getJobDefinition().getName();
                final JobInfo runningJob = jobInfoRepository.findByNameAndRunningState(name, RunningState.RUNNING);
                if (runningJob != null && runningJob.getHost().equals(InternetUtils.getHostName())) {
                    LOGGER.info("ltag=JobService.shutdownJobs jobInfoName={}", name);
                    jobInfoRepository.markAsFinished(runningJob.getId(), ResultCode.ABORTED, "shutdownJobs called from executing host");
                }
            }
        }
    }

    /**
     * public as we use it in tests also, don't use in other contexts
     */
    public void shutdownJobExecutorService(boolean recreate) {
        try {
            jobExecutorService.shutdown();
            jobExecutorService.awaitTermination(awaitTerminationSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("could not terminate all running threads");
        }
        if (recreate) {
            jobExecutorService = Executors.newCachedThreadPool();
        }
    }

    private boolean isExecutionDisabled() {
        return !isExecutionEnabled();
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
        return jobNames;
    }

    /**
     * Returns all registered JobRunnable Objects
     */
    public Collection<JobRunnable> listJobRunnables() {
        final List<JobRunnable> jobRunnables = new ArrayList<>(jobs.values());
        Collections.sort(jobRunnables, new Comparator<JobRunnable>() {
            @Override
            public int compare(JobRunnable o1, JobRunnable o2) {
                return o1.getJobDefinition().getName().compareTo(o2.getJobDefinition().getName());
            }
        });
        return jobRunnables;
    }

    /**
     * Returns the Set of all constraints
     */
    public Set<Set<String>> listRunningConstraints() {
        return Collections.unmodifiableSet(runningConstraints);
    }

    private boolean jobAgedOverInterval(Date lastModificationTime, long currentTime, long interval) {
        return new Date(currentTime - interval).after(lastModificationTime);
    }

    private void updateJobStatus(JobInfo jobInfo, JobRunnable runnable, RemoteJobStatus remoteJobStatus, JobDefinition jobDefinition) {
        LOGGER.info("ltag=JobService.updateJobStatus jobName={} jobId={} status={}", jobInfo.getName(), jobInfo.getId(), remoteJobStatus.status);
        if (remoteJobStatus.logLines != null && !remoteJobStatus.logLines.isEmpty()) {
            jobInfoRepository.appendLogLines(jobInfo.getId(), remoteJobStatus.logLines);
        }
        if (remoteJobStatus.message != null && remoteJobStatus.message.length() > 0) {
            jobInfoRepository.setStatusMessage(jobInfo.getId(), remoteJobStatus.message);
        }
        if (remoteJobStatus.status == RemoteJobStatus.Status.FINISHED) {
            LOGGER.info("ltag=JobService.updateJobStatus.statusFinish jobName={} result={}", jobInfo.getName(), remoteJobStatus.result);
            final JobExecutionContext context = createJobExecutionContext(jobInfo.getId(), jobDefinition, jobInfo.getExecutionPriority(), remoteJobStatus.logLines);
            context.setResultCode(remoteJobStatus.result.ok ? ResultCode.SUCCESSFUL : ResultCode.FAILED);
            context.setResultMessage(remoteJobStatus.message);
            if (remoteJobStatus.result.ok) {
                try {
                    runnable.afterExecution(context);
                    jobInfoRepository.markAsFinished(context.getId(), context.getResultCode(), remoteJobStatus.result.message);
                } catch (Exception e) {
                    LOGGER.error("ltag=JobService.updateJobStatus.afterExecution jobName=" + jobInfo.getName() + " jobId=" + jobInfo.getId() + " failed: " + e.getMessage(), e);
                    jobInfoRepository.markAsFinished(context.getId(), e);
                    runnable.onException(context, e, JobRunnable.State.AFTER_EXECUTION);
                }
            } else {
                LOGGER.warn("ltag=JobService.updateJobStatus.resultNotOk jobName={} jobId={} exitCode={} message={}",
                        jobInfo.getName(), jobInfo.getId(), remoteJobStatus.result.exitCode, remoteJobStatus.result.message);
                jobInfoRepository.addAdditionalData(jobInfo.getId(), "exitCode", String.valueOf(remoteJobStatus.result.exitCode));
                jobInfoRepository.markAsFinished(jobInfo.getId(), ResultCode.FAILED, remoteJobStatus.result.message);
                runnable.onException(context, new RemoteJobFailedException(jobInfo, remoteJobStatus), JobRunnable.State.EXECUTE);
            }
        }
    }

    private ExecutorService jobExecutorService = Executors.newCachedThreadPool();

    private void executeJob(JobRunnable runnable, String id, JobExecutionPriority executionPriority) {
        final JobDefinition definition = runnable.getJobDefinition();

        jobExecutorService.execute(new JobExecutionRunnable(
                runnable, jobInfoRepository, jobDefinitionRepository, createJobExecutionContext(id, definition, executionPriority, null)));
    }

    private JobExecutionContext createJobExecutionContext(String jobId, JobDefinition jobDefinition, JobExecutionPriority priority, List<String> logLines) {
        final JobLogger jobLogger = new SimpleJobLogger(jobId, jobInfoRepository, logLines);
        final JobInfoCache jobInfoCache = new JobInfoCache(jobId, jobInfoRepository, JOB_INFO_CACHE_UPDATE_INTERVAL);
        return new JobExecutionContext(jobId, jobLogger, jobInfoCache, priority, jobDefinition);
    }

    /**
     * paradigma:
     * - neue Job sofort als running markieren
     * - Danach auf running constraints pruefen
     * - wenn running constraints verletzt, dann job wieder zurueck auf queued
     */
    void executeQueuedJob(JobRunnable runnable, String id, JobExecutionPriority executionPriority) {
        final String name = runnable.getJobDefinition().getName();
        if (!jobInfoRepository.activateQueuedJobById(id)) {
            LOGGER.info("ltag=JobService.executeQueuedJob.activateQueuedJobFailed jobInfoName={} jobInfoId={}", name, id);
        } else if (violatesRunningConstraints(name, false)) {
            LOGGER.info("ltag=JobService.executeQueuedJob.violatesRunningConstraints jobInfoName={} jobInfoId={}", name, id);
            jobInfoRepository.deactivateRunningJob(id);
        } else {
            jobInfoRepository.updateHostThreadInformation(id);
            LOGGER.info("ltag=JobService.activateQueuedJob.activate jobInfoName={} jobInfoId={}", name, id);
            executeJob(runnable, id, executionPriority);
        }
    }

    private String queueJob(JobRunnable runnable, JobExecutionPriority jobExecutionPriority, Map<String, String> parameters, String exceptionMessage)
            throws JobAlreadyQueuedException {
        final String id = createJob(runnable, jobExecutionPriority, RunningState.QUEUED, parameters);
        if (id == null) {
            throw new JobAlreadyQueuedException(exceptionMessage);
        }
        return id;
    }

    private String runJob(JobRunnable runnable, JobExecutionPriority jobExecutionPriority, Map<String, String> parameters, String exceptionMessage)
            throws JobAlreadyRunningException {
        final String id = createJob(runnable, jobExecutionPriority, RunningState.RUNNING, parameters);
        if (id == null) {
            throw new JobAlreadyRunningException(exceptionMessage);
        }
        return id;
    }

    private String createJob(JobRunnable runnable, JobExecutionPriority jobExecutionPriority, RunningState runningState, Map<String, String> parameters) {
        final JobDefinition jobDefinition = runnable.getJobDefinition();
        // TODO: create-Methode mit JobRunnable in jobInfoRepository erzeugen
        return jobInfoRepository.create(jobDefinition.getName(), jobDefinition.getMaxIdleTime(), jobDefinition.getMaxExecutionTime(),
                jobDefinition.getMaxRetries(), runningState, jobExecutionPriority, parameters);
    }

    private void checkIfJobIsRegistered(final String name) throws JobNotRegisteredException {
        if (!isJobRegistered(name)) {
            throw new JobNotRegisteredException("job with name " + name + " is not registered with this jobService instance");
        }
    }

    private boolean violatesRunningConstraints(final String name, boolean alsoCheckForQueuedJobs) {
        for (Set<String> constraint : runningConstraints) {
            if (constraint.contains(name)) {
                for (String constraintJobName : constraint) {
                    if (name.equals(constraintJobName)) {
                        continue; // no self check here
                    }
                    if (jobInfoRepository.hasJob(constraintJobName, RunningState.RUNNING)) {
                        return true;
                    }
                    if (alsoCheckForQueuedJobs) {
                        if (jobInfoRepository.hasJob(constraintJobName, RunningState.QUEUED)) {
                            return true;
                        }

                    }
                }
            }
        }
        return false;
    }

    private boolean isJobEnabled(String name) {
        if (shutdown) return false;

        final StoredJobDefinition semaphore = getJobDefinition(name);
        return !semaphore.isDisabled();
    }

    private StoredJobDefinition getJobDefinition(String name) {
        return jobDefinitionRepository.find(name);
    }

    /**
     * Executes all queued jobs registered with this JobService instance asynchronously in the order they were queued.
     */
    void retryFailedJobs() {
        LOGGER.info("ltag=JobService.retryFailedJobs called");
        try {
            doRetryFailedJobs();
        } catch (Exception e) {
            LOGGER.error("ltag=JobService.retryFailedJobs exception occurred", e);
        }
        LOGGER.info("ltag=JobService.retryFailedJobs finished");
    }

    void doRetryFailedJobs() {
        if (!activeChecker.isActive()) {
            LOGGER.info("ltag=JobService not active");
            return;
        }
        desynchronize();
        for (JobRunnable jobRunnable : jobs.values()) {
            JobDefinition definition = jobRunnable.getJobDefinition();
            String name = definition.getName();
            long maxRetries = definition.getMaxRetries();

            if (maxRetries <= 0) {
                LOGGER.debug("ltag=JobService.retryFailedJobs jobInfoName={} no retries defined, skipping job", name);
                continue;
            }
            JobInfo jobInfo = jobInfoRepository.findMostRecentFinished(name);

            if (jobInfo == null) {
                LOGGER.debug("ltag=JobService.retryFailedJobs jobInfoName={} no last execution found, skipping job", name);
                continue;
            }

            final long retries = jobInfo.getRetries();

            if (retries > 0) {

                if (jobInfo.getResultState() == ResultCode.SUCCESSFUL) {
                    LOGGER.debug("ltag=JobService.retryFailedJobs jobInfoName={} last execution was resultCode={}, skipping job", name, jobInfo.getResultState());
                    continue;
                }

                if (!jobAgedOverInterval(jobInfo.getLastModifiedTime(), System.currentTimeMillis(), definition.getRetryInterval())) {
                    LOGGER.debug("ltag=JobService.retryFailedJobs jobInfoName={} did not yet reach retry interval time, skipping job", name);
                    continue;
                }

                // Prüfung, ob gerade ein job läuft, um doppeltes Starten/Queueing zu vermeiden
                final JobInfo runningJobInfo = jobInfoRepository.findByNameAndRunningState(name, RunningState.RUNNING);
                if (runningJobInfo != null) {
                    LOGGER.debug("ltag=JobService.retryFailedJobs jobInfoName={} found already running job, skipping job", name);
                    continue;
                }

                try {
                    String id = executeJob(name, jobInfo.getExecutionPriority());
                    LOGGER.debug("ltag=JobService.retryFailedJobs jobInfoName={} executeJob called", name);
                } catch (JobException e) {
                    LOGGER.error("ltag=JobService.retryFailedJobs jobInfoName={} executeJob failed: {}", name, e.getMessage());
                }
            }
        }
    }

    void cleanupOldJobs() {
        if (!activeChecker.isActive()) {
            LOGGER.info("ltag=JobService not active");
            return;
        }
        jobInfoRepository.cleanupOldJobs();
    }

    void cleanupTimedOutJobs() {
        if (!activeChecker.isActive()) {
            LOGGER.info("ltag=JobService not active");
            return;
        }
        jobInfoRepository.cleanupTimedOutJobs();
    }

    public JobDefinition getJobDefinitionByName(String jobName) {
        final JobRunnable jobRunnable = jobs.get(jobName);
        return (jobRunnable != null) ? jobRunnable.getJobDefinition() : null;
    }
}
