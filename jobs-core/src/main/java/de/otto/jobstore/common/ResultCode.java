package de.otto.jobstore.common;

/**
 * The Result code of a Job once it is finished.
 */
public enum ResultCode {

    SUCCESSFUL,

    FAILED,

    TIMED_OUT,

    ABORTED,

    /** If job gets skipped because preconditions not fulfilled */
    NOT_EXECUTED

}
