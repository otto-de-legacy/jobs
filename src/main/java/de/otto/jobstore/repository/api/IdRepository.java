package de.otto.jobstore.repository.api;

/**
 * Repository which supplies auto-incrementing ids
 */
public interface IdRepository extends Repository {

    /**
     * Returns an auto-incrementing id for the supplied name
     *
     * @param name The name for which the id should be requested
     * @return An id which is larger than the last id requested for the same name
     */
    Long getId(String name);

}
