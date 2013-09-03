package de.otto.jobstore.service;

import de.otto.jobstore.common.*;
import de.otto.jobstore.repository.JobDefinitionRepository;
import de.otto.jobstore.repository.JobInfoRepository;
import de.otto.jobstore.service.exception.JobExecutionAbortedException;
import de.otto.jobstore.service.exception.JobExecutionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

final class JobExecutionRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionRunnable.class);

    final JobRunnable jobRunnable;
    final JobInfoRepository jobInfoRepository;
    final JobDefinitionRepository jobDefinitionRepository;
    final JobExecutionContext context;

    JobExecutionRunnable(JobRunnable jobRunnable, JobInfoRepository jobInfoRepository, JobDefinitionRepository jobDefinitionRepository, JobExecutionContext context) {
        this.jobRunnable = jobRunnable;
        this.jobInfoRepository = jobInfoRepository;
        this.jobDefinitionRepository = jobDefinitionRepository;
        this.context = context;

    }

    @Override
    public void run() {
        final JobDefinition jobDefinition = jobRunnable.getJobDefinition();
        final String name = jobDefinition.getName();
        try {
            LOGGER.info("ltag=JobService.JobExecutionRunnable.run start jobName={} jobId={}", name, context.getId());
            if (jobRunnable.prepare(context)) {
                // save parameters directly before execution
                jobInfoRepository.saveParameters(context.getId(),jobRunnable.getParameters());
                jobRunnable.execute(context);
                if (!jobDefinition.isRemote()) {
                    LOGGER.info("ltag=JobService.JobExecutionRunnable.run finished jobName={} jobId={}", name, context.getId());
                    jobRunnable.afterExecution(context);
                    jobInfoRepository.markAsFinished(context.getId(), context.getResultCode(), context.getResultMessage());
                }
            } else {
                LOGGER.info("ltag=JobService.JobExecutionRunnable.run skipped jobName={} jobId={}", name, context.getId());
                jobInfoRepository.remove(context.getId());
                jobDefinitionRepository.setLastNotExecuted(name, new Date());
            }
        } catch (JobExecutionAbortedException e) {
            LOGGER.warn("ltag=JobService.JobExecutionRunnable.run jobName=" + name + " jobId=" + context.getId() + " was aborted");
            jobInfoRepository.markAsFinished(context.getId(), ResultCode.ABORTED);
        } catch (JobExecutionTimeoutException e) {
            LOGGER.warn("ltag=JobService.JobExecutionRunnable.run jobName=" + name + " jobId=" + context.getId() + " timed out");
            jobInfoRepository.markAsFinished(context.getId(), ResultCode.TIMED_OUT);
        } catch (Exception e) {
            LOGGER.error("ltag=JobService.JobExecutionRunnable.run jobName=" + name + " jobId=" + context.getId() + " failed: " + e.getMessage(), e);
            jobInfoRepository.markAsFinished(context.getId(), e);
        }
    }

}
