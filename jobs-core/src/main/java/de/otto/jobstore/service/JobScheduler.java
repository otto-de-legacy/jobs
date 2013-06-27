package de.otto.jobstore.service;

import de.otto.jobstore.common.JobSchedule;
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
 * method to unify scheduling. This spawns some extra daemon threads
 */
public class JobScheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobScheduler.class);

    private List<JobSchedule> schedules;

    public JobScheduler(final JobService jobService, final JobInfoRepository jobInfoRepository) {
        this(createDefaultSchedules(jobService, jobInfoRepository));
    }

    public JobScheduler(List<JobSchedule> schedules) {
        this.schedules = schedules;
    }

    private ScheduledExecutorService executorService;

    @PostConstruct
    public synchronized void startup() {
        LOGGER.info("called startup");

        if(executorService != null) {
            shutdown();
        }

        executorService = Executors.newScheduledThreadPool(schedules.size(),new JobSchedulerThreadFactory());

        for(JobSchedule schedule: schedules) {
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


        try {
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("error await termination of tasks: " + e.getMessage(), e);
        }
        executorService = null;
        LOGGER.info("finished shutdown");
    }

    private static List<JobSchedule> createDefaultSchedules(final JobService jobService, final JobInfoRepository jobInfoRepository) {
        List<JobSchedule> schedules = new ArrayList<>();
        schedules.add(new JobSchedule() {
            @Override
            public long interval() {
                return TimeUnit.MINUTES.toMillis(5);
            }

            @Override
            public void schedule() {
                jobInfoRepository.cleanupTimedOutJobs();
            }

            @Override
            public String getName() {
                return "jobInfoRepository.cleanupTimedOutJobs()";
            }
        });

        schedules.add(new JobSchedule() {
            @Override
            public long interval() {
                return TimeUnit.DAYS.toMillis(1);
            }

            @Override
            public void schedule() {
                jobInfoRepository.cleanupOldJobs();
            }

            @Override
            public String getName() {
                return "jobInfoRepository.cleanupOldJobs()";
            }
        });

        schedules.add(new JobSchedule() {
            @Override
            public long interval() {
                return TimeUnit.MINUTES.toMillis(1);
            }

            @Override
            public void schedule() {
                jobService.executeQueuedJobs();
            }
            @Override
            public String getName() {
                return "jobService.executeQueuedJobs()";
            }
        });

        schedules.add(new JobSchedule() {
            @Override
            public long interval() {
                return TimeUnit.MINUTES.toMillis(1);
            }
            @Override
            public void schedule() {
                jobService.pollRemoteJobs();
            }
            @Override
            public String getName() {
                return "jobService.pollRemoteJobs()";
            }
        });

        schedules.add(new JobSchedule() {
            @Override
            public long interval() {
                return TimeUnit.MINUTES.toMillis(1);
            }
            @Override
            public void schedule() {
                jobService.retryFailedJobs();
            }
            @Override
            public String getName() {
                return "jobService.retryFailedJobs()";
            }
        });

        return schedules;
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
