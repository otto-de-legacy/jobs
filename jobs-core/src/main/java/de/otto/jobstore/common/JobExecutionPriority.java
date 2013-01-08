package de.otto.jobstore.common;


public enum JobExecutionPriority {

    CHECK_PRECONDITIONS,
    IGNORE_PRECONDITIONS,
    FORCE_EXECUTION;

    public boolean isLowerThan(JobExecutionPriority priority) {
        return this.compareTo(priority) < 0;
    }

    public boolean isEqualOrHigherThan(JobExecutionPriority priority) {
        return this.compareTo(priority) >= 0;
    }

}
