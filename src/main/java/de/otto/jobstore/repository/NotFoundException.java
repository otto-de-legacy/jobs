package de.otto.jobstore.repository;

/**
 * Exception to Signal that the requested Element could not be found
 */
public final class NotFoundException extends RuntimeException {

    public NotFoundException(String message) {
        super(message);
    }

}
