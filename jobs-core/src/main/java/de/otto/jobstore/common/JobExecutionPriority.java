package de.otto.jobstore.common;


public enum JobExecutionPriority {

    CHECK_PRECONDITIONS(1),
    IGNORE_PRECONDITIONS(2),
    FORCE_EXECUTION(3);

    private final int level;

    private JobExecutionPriority(int level) {
        this.level = level;
    }

    public boolean hasLowerPriority(JobExecutionPriority priority) {
        return this.level < priority.level;
    }

}
