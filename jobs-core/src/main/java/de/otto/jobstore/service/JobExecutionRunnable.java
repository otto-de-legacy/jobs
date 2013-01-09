package de.otto.jobstore.service;

import de.otto.jobstore.common.*;
import de.otto.jobstore.repository.JobInfoRepository;
import de.otto.jobstore.service.exception.JobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JobExecutionRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobService.class);

    final JobRunnable jobRunnable;
    final JobInfoRepository jobInfoRepository;
    final JobExecutionContext context;

    JobExecutionRunnable(JobRunnable jobRunnable, JobInfoRepository jobInfoRepository, JobExecutionContext context) {
        this.jobRunnable = jobRunnable;
        this.jobInfoRepository = jobInfoRepository;
        this.context = context;
    }

    @Override
    public void run() {
        try {
            LOGGER.info("ltag=JobService.JobExecutionRunnable.run start jobName={}", jobRunnable.getName());
            jobRunnable.beforeExecution(context);
            jobRunnable.execute(context);
            // Check because an asynchronous job may still be running
            if (context.getRunningState() == RunningState.FINISHED) {
                LOGGER.info("ltag=JobService.JobExecutionRunnable.run finished jobName={}", jobRunnable.getName());
                jobRunnable.afterExecution(context);
                jobInfoRepository.markRunningAsFinished(jobRunnable.getName(), context.getResultCode(), context.getResultMessage());
            }
        } catch (Exception e) {
            LOGGER.error("ltag=JobService.JobExecutionRunnable.run jobName=" + jobRunnable.getName()+" failed: "+e.getMessage(),e);
            jobInfoRepository.markRunningAsFinishedWithException(jobRunnable.getName(), e);
        }
    }

}
