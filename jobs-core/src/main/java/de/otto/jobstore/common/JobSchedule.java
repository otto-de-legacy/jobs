package de.otto.jobstore.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JobSchedule implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobSchedule.class);

    private long count=0;
    private long lastExecuted=0;

    public long count() {
        return count;
    }

    @Override
    public synchronized void run() {
        try {
            count++;
            lastExecuted = System.currentTimeMillis();
            LOGGER.info("schedule called on {} ({})", getName(), count);
            schedule();
        } catch (Exception e) {
            LOGGER.error("error executing JobSchedule {}", getName(), e);
        }
        LOGGER.info("schedule finished on {} ({})", getName(),count);
    }

    public abstract long interval();

    public abstract void schedule();

    public abstract String getName();

    public static JobSchedule create(final String name, final long interval, final Runnable runnable) {
        return new JobSchedule() {
            @Override
            public long interval() {
                return interval;
            }

            @Override
            public void schedule() {
                if(runnable == null) {
                    LOGGER.warn("runnable is null");
                } else {
                    runnable.run();
                }
            }

            @Override
            public String getName() {
                return name;
            }
        };
    }
}