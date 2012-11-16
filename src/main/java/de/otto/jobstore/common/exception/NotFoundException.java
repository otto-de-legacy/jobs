package de.otto.jobstore.common.exception;

/**
 * Exception to Signal that the requested Element could not be found
 */
public final class NotFoundException extends RuntimeException {

    public NotFoundException(String message) {
        super(message);
    }

}
