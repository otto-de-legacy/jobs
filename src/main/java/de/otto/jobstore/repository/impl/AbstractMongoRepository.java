package de.otto.jobstore.repository.impl;

import com.mongodb.*;
import de.otto.jobstore.common.Item;
import de.otto.jobstore.repository.api.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract Class to access a MongoDb
 * @param <E> The Class to be stored in the MongoDb, has to be of type {@link de.otto.jobstore.common.Item}
 */
abstract class AbstractMongoRepository<E extends Item> implements Repository {

    final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    final DBCollection collection;

    AbstractMongoRepository(Mongo mongo, String dbName, String collectionName) {
        this(mongo, dbName, collectionName, null, null);
    }

    AbstractMongoRepository(Mongo mongo, String dbName, String collectionName, String username, String password) {
        final DB db = mongo.getDB(dbName);
        if (username != null && !username.isEmpty()) {
            if (db.isAuthenticated()) {
                final boolean authenticateSuccess = db.authenticate(username, password.toCharArray());
                if (!authenticateSuccess) {
                    throw new RuntimeException("The authentication at the database: " + dbName + " on the host: " +
                            mongo.getAddress() + " with the username: " + username + " and the given password was not successful");
                } else {
                    LOGGER.info("Login at database {} on the host {} was successful", dbName, mongo.getAddress());
                }
            }
        }
        collection = db.getCollection(collectionName);
        LOGGER.info("Prepare access to MongoDB collection '{}' on {}/{}", new Object[]{collectionName, mongo, dbName});
        prepareCollection();
    }

    public final void clear(boolean dropCollection) {
        LOGGER.info("Going to clear all entities on collection: {}", collection.getFullName());
        if (dropCollection) {
            collection.drop();
        } else {
            final WriteResult wr = collection.remove(new BasicDBObject());
            final CommandResult cr = wr.getLastError(WriteConcern.SAFE);
            if (cr.ok()) {
                LOGGER.info("Cleared all entities successfully on collection: {}", collection.getFullName());
            } else {
                LOGGER.error("Could not clear entities on collection {}: {}", collection.getFullName(), cr.getErrorMessage());
            }
        }
        prepareCollection();
    }

    public final long count() {
        return collection.count();
    }

    protected abstract void prepareCollection();

    final void save(E item) {
        collection.save(item.toDbObject());
    }

    final void save(E item, WriteConcern writeConcern) {
        collection.save(item.toDbObject(), writeConcern);
    }

}
