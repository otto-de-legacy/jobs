package de.otto.jobstore.common.example;


import de.otto.jobstore.common.AbstractLocalJobRunnable;
import de.otto.jobstore.common.JobExecutionContext;
import de.otto.jobstore.common.JobLogger;
import de.otto.jobstore.service.exception.JobExecutionException;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

public final class SimpleJobRunnableExample extends AbstractLocalJobRunnable {

    /**
     * @return name of the simple job, might differ from Classname
     */
    public String getName() {
        return "SimpleJobRunnableExampleToBeUsed";
    }

    /**
     * Job should be considered timed out after 10 minutes
     */
    @Override
    public long getMaxExecutionTime() {
        return 1000 * 60 * 10;
    }

    /**
     * Job should only run on a Sunday
     */
    @Override
    public boolean isExecutionNecessary() {
        return new GregorianCalendar().get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY;
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
        Random r = new Random();
        for (int i = 0; i < 100; i++) {
            final int randomNumber = r.nextInt();
            if (randomNumber == 0) {
                throw new IllegalArgumentException("Division by Zero");
            } else {
                executionContext.getJobLogger().addLoggingData("Computed the number: " + i / randomNumber);
            }
        }
    }

}
