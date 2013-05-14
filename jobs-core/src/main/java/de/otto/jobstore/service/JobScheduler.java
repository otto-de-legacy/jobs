package de.otto.jobstore.service;

import de.otto.jobstore.repository.JobInfoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * method to unify scheduling. Should be either called frequently per scheduler once a minute OR
 * use the factory method. This spawns an extra thread,
 */
public class JobScheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobScheduler.class);

    private final JobService jobService;
    private final JobInfoRepository jobInfoRepository;
    private List<Schedule> schedules = new ArrayList<>();

    public JobScheduler(JobService jobService, JobInfoRepository jobInfoRepository) {
        this.jobService = jobService;
        this.jobInfoRepository = jobInfoRepository;
        initSchedules();
    }

    private ScheduledExecutorService executorService;

    @PostConstruct
    public synchronized void startup() {
        LOGGER.info("called startup");

        if(executorService != null) {
            shutdown();
        }

        executorService = Executors.newScheduledThreadPool(schedules.size(),new JobSchedulerThreadFactory());

        for(Schedule schedule: schedules) {
            executorService.scheduleAtFixedRate(schedule, 0, schedule.interval(), TimeUnit.MILLISECONDS);
        }

        LOGGER.info("finished startup");
    }

    @PreDestroy
    public synchronized void shutdown() {
        LOGGER.info("called shutdown");

        if(executorService == null) {
            LOGGER.info("shutdown: executor service already removed, stop here.");
            return;
        }

        executorService.shutdownNow();
        try {
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("error rescheduling tasks: " + e.getMessage(), e);
        }
        executorService = null;
        LOGGER.info("finished shutdown");
    }

    private void initSchedules() {
        schedules.add(new Schedule() {
            @Override
            long interval() {
                return TimeUnit.MINUTES.toMillis(5);
            }

            @Override
            void schedule() {
                jobInfoRepository.cleanupTimedOutJobs();
            }

            @Override
            String getName() {
                return "jobInfoRepository.cleanupTimedOutJobs()";
            }
        });

        schedules.add(new Schedule() {
            @Override
            long interval() {
                return TimeUnit.DAYS.toMillis(1);
            }

            @Override
            void schedule() {
                jobInfoRepository.cleanupOldJobs();
            }

            @Override
            String getName() {
                return "jobInfoRepository.cleanupOldJobs()";
            }
        });

        schedules.add(new Schedule() {
            @Override
            long interval() {
                return TimeUnit.HOURS.toMillis(1);
            }

            @Override
            void schedule() {
                jobInfoRepository.cleanupNotExecutedJobs();
            }

            @Override
            String getName() {
                return "jobInfoRepository.cleanupNotExecutedJobs()";
            }
        });

        schedules.add(new Schedule() {
            @Override
            long interval() {
                return TimeUnit.MINUTES.toMillis(1);
            }

            @Override
            void schedule() {
                jobService.executeQueuedJobs();
            }

            @Override
            String getName() {
                return "jobService.executeQueuedJobs()";
            }
        });

        schedules.add(new Schedule() {
            @Override
            long interval() {
                return TimeUnit.MINUTES.toMillis(1);
            }

            @Override
            void schedule() {
                jobService.pollRemoteJobs();
            }
            @Override
            String getName() {
                return "jobService.pollRemoteJobs()";
            }
        });

        schedules.add(new Schedule() {
            @Override
            long interval() {
                return TimeUnit.MINUTES.toMillis(1);
            }

            @Override
            void schedule() {
                jobService.retryFailedJobs();
            }
            @Override
            String getName() {
                return "jobService.retryFailedJobs()";
            }
        });

    }

    private abstract static class Schedule implements Runnable {

        @Override
        public void run() {

            try {
                LOGGER.info("schedule called on {}",getName());
                schedule();
            } catch(Exception e) {
                LOGGER.error("error executing Scheduled {}",getName(),e);
            }
            LOGGER.info("schedule finished on {}",getName());
        }

        abstract long interval();
        abstract void schedule();
        abstract String getName();
    }

    /**
     * shameless copy of Executors.DefaultThreadFactory with some adjustments:
     * - changed name prefix
     * - threads are daemon threads
     * - threads run with MAX_PRIORITY
     *
     */
    private static class JobSchedulerThreadFactory implements ThreadFactory {
        private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        JobSchedulerThreadFactory() {
            final SecurityManager s = System.getSecurityManager();
            group = (s == null) ? Thread.currentThread().getThreadGroup() : s.getThreadGroup();
            namePrefix = "jobScheduler-" + POOL_NUMBER.getAndIncrement() + "-thread-";
        }

        public Thread newThread(Runnable r) {
            final Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (!t.isDaemon()) {
                t.setDaemon(true);
            }
            if (t.getPriority() != Thread.MAX_PRIORITY) {
                t.setPriority(Thread.MAX_PRIORITY);
            }
            return t;
        }
    }


}
