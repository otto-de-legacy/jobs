package de.otto.jobstore.common.example;

import de.otto.jobstore.common.*;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionAbortedException;
import de.otto.jobstore.service.exception.JobExecutionException;

public class SimpleAbortableJob extends AbstractLocalJobRunnable {


    @Override
    public JobDefinition getJobDefinition() {
        return new AbstractLocalJobDefinition() {
            @Override
            public String getName() {
                return "SimpleAbortableJob";
            }

            @Override
            public long getMaxExecutionTime() {
                return 1000;
            }

            @Override
            public long getMaxIdleTime() {
                return 1000;
            }

            @Override
            public boolean isAbortable() {
                return true;
            }
        };
    }

    @Override
    public void execute(JobExecutionContext context) throws JobException {
        for (int i = 0; i < 1000000; i++) {
            context.checkForAbort();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new JobExecutionException(e.getMessage(), e);
            }
        }
    }
}
