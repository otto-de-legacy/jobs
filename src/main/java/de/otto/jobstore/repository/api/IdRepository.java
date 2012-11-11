package de.otto.jobstore.repository.api;

public interface IdRepository {

    Long getId(String collectionName);

    void clear(boolean dropCollection);

}
