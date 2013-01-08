package de.otto.jobstore.service;


import de.otto.jobstore.common.JobExecutionResult;
import de.otto.jobstore.common.JobExecutionContext;
import de.otto.jobstore.common.JobRunnable;
import de.otto.jobstore.common.RunningState;
import de.otto.jobstore.repository.JobInfoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JobExecutionRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobService.class);

    final JobRunnable jobRunnable;
    final JobInfoRepository jobInfoRepository;
    final JobExecutionContext executionContext;

    JobExecutionRunnable(JobRunnable jobRunnable, JobInfoRepository jobInfoRepository, JobExecutionContext context) {
        this.jobRunnable = jobRunnable;
        this.jobInfoRepository = jobInfoRepository;
        this.executionContext = context;
    }

    @Override
    public void run() {
        try {
            LOGGER.info("ltag=JobService.JobExecutionRunnable.run jobInfoName={}", jobRunnable.getName());
            final JobExecutionResult result = jobRunnable.execute(executionContext);
            if (RunningState.FINISHED.equals(result.getRunningState())) {
                jobInfoRepository.markRunningAsFinished(jobRunnable.getName(), result.getResultCode(), null);
            }
        } catch (Exception e) {
            LOGGER.error("Job: " + jobRunnable.getName()+" finished with exception: "+e.getMessage(),e);
            jobInfoRepository.markRunningAsFinishedWithException(jobRunnable.getName(), e);
        } catch (Error e) {
            jobInfoRepository.markRunningAsFinishedWithException(jobRunnable.getName(), e);
            throw e;
        }
    }

}
