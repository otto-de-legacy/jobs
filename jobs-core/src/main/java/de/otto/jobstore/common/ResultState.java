package de.otto.jobstore.common;

/**
 * The State a Job can have once it is finished
 */
public enum ResultState {

    SUCCESSFUL,
    FAILED,
    TIMED_OUT,
    NOT_EXECUTED

}
