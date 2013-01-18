package de.otto.jobstore.common;

import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.service.JobInfoService;
import de.otto.jobstore.service.RemoteJobExecutorService;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collections;
import java.util.Date;

public abstract class AbstractRemoteJobRunnable implements JobRunnable {

    protected Logger log = LoggerFactory.getLogger(this.getClass());

    protected final RemoteJobExecutorService remoteJobExecutorService;
    protected final JobInfoService jobInfoService;

    protected AbstractRemoteJobRunnable(RemoteJobExecutorService remoteJobExecutorService, JobInfoService jobInfoService) {
        this.remoteJobExecutorService = remoteJobExecutorService;
        this.jobInfoService = jobInfoService;
    }

    @Override
    public RemoteJobStatus getRemoteStatus(JobExecutionContext context) {
        final String remoteJobUri = context.getJobLogger().getAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val());
        if (remoteJobUri == null) {
            return new RemoteJobStatus(RemoteJobStatus.Status.FINISHED, Collections.<String>emptyList(),
                    new RemoteJobResult(false, 1, "RemoteJobUri is not set, cannot continue."), new Date().toString());
        }
        final RemoteJobStatus status = remoteJobExecutorService.getStatus(URI.create(remoteJobUri));
        final JobInfo jobInfo = jobInfoService.getById(context.getId());

        if (jobInfo != null && status.logLines != null
                && jobInfo.getLogLines() != null && !jobInfo.getLogLines().isEmpty()) {
            final int currentLength = jobInfo.getLogLines().size();
            // Assume that old lines are already included, and therefore can be cut off
            if (currentLength <= status.logLines.size()) {
                status.logLines = status.logLines.subList(currentLength, status.logLines.size());
            }
        }
        return status;
    }

    /**
     * By default returns true, override for your custom needs,
     */
    @Override
    public boolean prepare(JobExecutionContext context) {
        return true;
    }

    /**
     * Only triggers the remote job, poll to check wether job is finished or not.
     * @see de.otto.jobstore.service.JobService#pollRemoteJobs()
     */
    @Override
    public void execute(JobExecutionContext context) throws JobException {
        final JobLogger jobLogger = context.getJobLogger();
        try {
            log.info("Trigger remote job '{}' [{}] ...", getJobDefinition().getName(), context.getId());
            final URI uri = remoteJobExecutorService.startJob(new RemoteJob(getJobDefinition().getName(), context.getId(), getParameters()));
            jobLogger.insertOrUpdateAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), uri.toString());
        } catch (RemoteJobAlreadyRunningException e) {
            log.info("Remote job '{}' [{}] is already running: " + e.getMessage(), getJobDefinition().getName(), context.getId());
            jobLogger.insertOrUpdateAdditionalData("resumedAlreadyRunningJob", e.getJobUri().toString());
            jobLogger.insertOrUpdateAdditionalData(JobInfoProperty.REMOTE_JOB_URI.val(), e.getJobUri().toString());
        }
    }

    /**
     * Implementation might want to set the {@link JobExecutionContext#resultCode}
     */
    @Override
    public void afterExecution(JobExecutionContext context) throws JobException {
    }

}
