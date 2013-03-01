package de.otto.jobstore.common.example;

import de.otto.jobstore.common.*;
import de.otto.jobstore.service.exception.JobExecutionException;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Random;

public final class SimpleJobRunnableExample extends AbstractLocalJobRunnable {

    @Override
    public JobDefinition getJobDefinition() {
        return new AbstractLocalJobDefinition() {
            @Override
            public String getName() {
                return "SimpleJobRunnableExampleToBeUsed";
            }

            @Override
            public long getMaxExecutionTime() {
                return 1000 * 60 * 10;
            }

            @Override
            public long getMaxIdleTime() {
                return 1000 * 60 * 10;
            }
        };
    }

    @Override
    public Map<String, String> getParameters() {
        return null;
    }

    /**
     * Computes 100 numbers by dividing each number from 0 to 99 by a random number
     *
     * @param executionContext The context in which this job is executed
     * @throws JobExecutionException An exception is thrown if the random number is zero
     *                               and would thus cause a division by zero error
     */
    @Override
    public void execute(JobExecutionContext executionContext) throws JobExecutionException {
        if (JobExecutionPriority.CHECK_PRECONDITIONS.equals(executionContext.getExecutionPriority())
                || new GregorianCalendar().get(Calendar.DAY_OF_WEEK) != Calendar.SUNDAY) {
            executionContext.setResultCode(ResultCode.NOT_EXECUTED);
        }
        Random r = new Random();
        for (int i = 0; i < 100; i++) {
            final int randomNumber = r.nextInt();
            if (randomNumber == 0) {
                throw new IllegalArgumentException("Division by Zero");
            } else {
                executionContext.getJobLogger().addLoggingData("Computed the number: " + i / randomNumber);
            }
        }
        executionContext.setResultCode(ResultCode.SUCCESSFUL);
    }

}
