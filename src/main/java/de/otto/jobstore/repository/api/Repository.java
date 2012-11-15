package de.otto.jobstore.repository.api;


public interface Repository {

    /**
     * Clears all elements from the repository
     *
     * @param dropCollection Flag if the collection should be dropped
     */
    void clear(boolean dropCollection);

    /**
     * Counts the number of documents in the repository
     *
     * @return The number of documents in the repository
     */
    long count();

}
