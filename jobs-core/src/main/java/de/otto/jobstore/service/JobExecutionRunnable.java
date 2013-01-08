package de.otto.jobstore.service;

import de.otto.jobstore.common.JobLogger;
import de.otto.jobstore.common.JobRunnable;
import de.otto.jobstore.repository.JobInfoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JobExecutionRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobService.class);

    final JobRunnable jobRunnable;
    final JobInfoRepository jobInfoRepository;

    JobExecutionRunnable(JobRunnable jobRunnable, JobInfoRepository jobInfoRepository) {
        this.jobRunnable = jobRunnable;
        this.jobInfoRepository = jobInfoRepository;
    }

    @Override
    public void run() {
        try {
            JobLogger jobLogger = new SimpleJobLogger(jobRunnable.getName(), jobInfoRepository);
            LOGGER.info("ltag=JobService.JobExecutionRunnable.run jobInfoName={}", jobRunnable.getName());
            jobRunnable.executeOnStart(jobLogger);
            jobRunnable.execute(jobLogger);
            jobInfoRepository.markRunningAsFinishedSuccessfully(jobRunnable.getName());
            jobRunnable.executeOnSuccess(jobLogger);
        } catch (Exception e) {
            LOGGER.error("Job: " + jobRunnable.getName()+" finished with exception: "+e.getMessage(),e);
            jobInfoRepository.markRunningAsFinishedWithException(jobRunnable.getName(), e);
        } catch (Error e) {
            jobInfoRepository.markRunningAsFinishedWithException(jobRunnable.getName(), e);
            throw e;
        }
    }

}
