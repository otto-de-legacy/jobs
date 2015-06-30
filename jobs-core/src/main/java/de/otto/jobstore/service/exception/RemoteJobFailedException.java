package de.otto.jobstore.service.exception;

import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.RemoteJobStatus;

public class RemoteJobFailedException extends JobException {
    private final JobInfo jobInfo;
    private final RemoteJobStatus remoteJobStatus;

    public RemoteJobFailedException(JobInfo jobInfo, RemoteJobStatus remoteJobStatus) {
        super("Job " + jobInfo.getName() + " failed.");
        this.jobInfo = jobInfo;
        this.remoteJobStatus = remoteJobStatus;
    }

    public RemoteJobStatus getRemoteJobStatus() {
        return remoteJobStatus;
    }

    public JobInfo getJobInfo() {
        return jobInfo;
    }
}
