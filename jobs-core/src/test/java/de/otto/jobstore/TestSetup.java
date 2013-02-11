package de.otto.jobstore;


import de.otto.jobstore.common.*;
import de.otto.jobstore.service.JobInfoService;
import de.otto.jobstore.service.RemoteJobExecutorService;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;

import java.util.Map;

public class TestSetup {

    public static LocalMockJobRunnable localJobRunnable(final String name, final int timeout) {
        return localJobRunnable(name, timeout, null);
    }

    public static LocalMockJobRunnable localJobRunnable(final String name, final int timeout, final JobExecutionException exception) {
        return new LocalMockJobRunnable(name, timeout, exception);
    }

    public static AbstractRemoteJobRunnable remoteJobRunnable(final RemoteJobExecutorService remoteJobExecutorService, final JobInfoService jobInfoService,
                                                              final Map<String, String> parameters, final AbstractRemoteJobDefinition jobDefinition) {
        return new AbstractRemoteJobRunnable(remoteJobExecutorService, jobInfoService) {

            @Override
            public Map<String, String> getParameters() {
                return parameters;
            }

            @Override
            public JobDefinition getJobDefinition() {
                return jobDefinition;
            }

        };
    }

    public static AbstractLocalJobDefinition localJobDefinition(final String name, final long maxExecutionTime) {
        return new AbstractLocalJobDefinition() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public long getTimeoutPeriod() {
                return maxExecutionTime;
            }
        };
    }

    public static AbstractRemoteJobDefinition remoteJobDefinition(final String name, final long maxExecutionTime, final long pollingInterval) {
        return new AbstractRemoteJobDefinition() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public long getTimeoutPeriod() {
                return maxExecutionTime;
            }

            @Override
            public long getPollingInterval() {
                return pollingInterval;
            }
        };
    }

    public static class LocalMockJobRunnable extends AbstractLocalJobRunnable {

        private volatile boolean executed = false;
        private JobException exception;
        private AbstractLocalJobDefinition localJobDefinition;

        private LocalMockJobRunnable(String name, long maxExecutionTime, JobException exception) {
            localJobDefinition = localJobDefinition(name, maxExecutionTime);
            this.exception = exception;
        }

        @Override
        public JobDefinition getJobDefinition() {
            return localJobDefinition;
        }

        @Override
        public void execute(JobExecutionContext executionContext) throws JobException {
            executed = true;
            if (exception != null) {
                throw exception;
            }
        }

        public boolean isExecuted() {
            return executed;
        }

    }
}
