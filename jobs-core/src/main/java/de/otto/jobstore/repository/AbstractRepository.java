package de.otto.jobstore.repository;

import com.mongodb.*;
import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobInfoProperty;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public abstract class AbstractRepository<E extends AbstractItem> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final DBCollection collection;

    private WriteConcern safeWriteConcern = WriteConcern.SAFE;

    public AbstractRepository(final Mongo mongo, final String dbName, final String collectionName) {
        this(mongo, dbName, collectionName, null, null);
    }

    public AbstractRepository(final Mongo mongo, final String dbName, final String collectionName, final String username, final String password) {
        final DB db = mongo.getDB(dbName);
        if (username != null && !username.isEmpty()) {
            if (!db.isAuthenticated()) {
                final boolean authenticateSuccess = db.authenticate(username, password.toCharArray());
                if (!authenticateSuccess) {
                    throw new RuntimeException("The authentication at the database: " + dbName + " on the host: " +
                            mongo.getAddress() + " with the username: " + username + " and the given password was not successful");
                } else {
                    logger.info("Login at database {} on the host {} was successful", dbName, mongo.getAddress());
                }
            }
        }
        collection = db.getCollection(collectionName);
        logger.info("Prepare access to MongoDB collection '{}' on {}/{}", new Object[]{collectionName, mongo, dbName});
        prepareCollection();
    }

    public AbstractRepository(final Mongo mongo, final String dbName, final String collectionName, final String username, final String password, WriteConcern safeWriteConcern) {
        this(mongo, dbName, collectionName, username, password);
        if(safeWriteConcern == null) {
            throw new NullPointerException("writeConcern may not be null");
        }
        this.safeWriteConcern = safeWriteConcern;
    }

    public WriteConcern getSafeWriteConcern() {
        return safeWriteConcern;
    }

    public void save(E item) {
        final DBObject obj = item.toDbObject();
        final WriteResult wr = collection.save(obj,getSafeWriteConcern());
        final CommandResult cr = wr.getLastError();
        if (!cr.ok()) {
            logger.error("Unable to save job info object={} wr={}: ", wr, obj);
        }
    }

    /**
     * Clears all elements from the repository
     *
     * @param dropCollection Flag if the collection should be dropped
     */
    public final void clear(final boolean dropCollection) {
        logger.info("Going to clear all entities on collection: {}", collection.getFullName());
        if (dropCollection) {
            collection.drop();
            prepareCollection();
        } else {
            final WriteResult wr = collection.remove(new BasicDBObject());
            final CommandResult cr = wr.getLastError(getSafeWriteConcern());
            if (cr.ok()) {
                logger.info("Cleared all entities successfully on collection: {}", collection.getFullName());
            } else {
                logger.error("Could not clear entities on collection {}: {}", collection.getFullName(), cr.getErrorMessage());
            }
        }
    }

    // ~~

    abstract protected void prepareCollection();

    abstract protected E fromDbObject(DBObject dbObject);

    protected List<E> getAll(final DBCursor cursor) {
        final List<E> elements = new ArrayList<>();
        while (cursor.hasNext()) {
            elements.add(fromDbObject(cursor.next()));
        }
        return elements;
    }

    protected E getFirst(final DBCursor cursor) {
        if (cursor.hasNext()) {
            return fromDbObject(cursor.next());
        }
        return null;
    }

}
