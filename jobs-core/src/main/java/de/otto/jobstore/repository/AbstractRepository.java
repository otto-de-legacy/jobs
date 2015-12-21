package de.otto.jobstore.repository;

import com.mongodb.*;
import de.otto.jobstore.common.AbstractItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractRepository<E extends AbstractItem> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final DBCollection collection;

    private WriteConcern safeWriteConcern = WriteConcern.SAFE;

    public AbstractRepository(MongoClient mongoClient, String dbName, String collectionName) {
        this.collection = mongoClient.getDB(dbName).getCollection(collectionName);
        logger.info("Prepare access to MongoDB collection '{}' on {}/{}", collectionName, mongoClient, dbName);
        prepareCollection();
    }

    public AbstractRepository(MongoClient mongo, String dbName, String collectionName, WriteConcern safeWriteConcern) {
        this(mongo, dbName, collectionName);
        if(safeWriteConcern == null) {
            throw new NullPointerException("writeConcern may not be null");
        }
        this.safeWriteConcern = safeWriteConcern;
    }

    @Deprecated
    static MongoClient createMongoClient(Mongo mongo, String dbName, String username, String password) {
        MongoOptions mongoOptions = mongo.getMongoOptions();
        return new MongoClient(mongo.getAllAddress(),
                credentials(dbName, username, password),
                mongoClientOptions(mongoOptions));
    }

    @Deprecated
    private static MongoClientOptions mongoClientOptions(MongoOptions options) {
        MongoClientOptions.Builder builder = MongoClientOptions.builder()
                .connectionsPerHost(options.getConnectionsPerHost())
                .threadsAllowedToBlockForConnectionMultiplier(options.getThreadsAllowedToBlockForConnectionMultiplier())
                .maxWaitTime(options.getMaxWaitTime())
                .connectTimeout(options.getConnectTimeout())
                .socketTimeout(options.getSocketTimeout())
                .socketKeepAlive(options.isSocketKeepAlive())
                ;

        if (options.getReadPreference() != null) {
            builder.readPreference(options.getReadPreference());
        }
        if (options.getDbDecoderFactory() != null) {
            builder.dbDecoderFactory(options.getDbDecoderFactory());
        }
        if (options.getDbEncoderFactory() != null) {
            builder.dbEncoderFactory(options.getDbEncoderFactory());
        }
        if (options.getSocketFactory() != null) {
            builder.socketFactory(options.getSocketFactory());
        }
        if (options.getSocketFactory() != null) {
            builder.socketFactory(options.getSocketFactory());
        }
        if (options.getWriteConcern() != null) {
            builder.writeConcern(options.getWriteConcern());
        }
        return builder
                .description(options.getDescription())
                .cursorFinalizerEnabled(options.isCursorFinalizerEnabled())
                .alwaysUseMBeans(options.isAlwaysUseMBeans())
                .requiredReplicaSetName(options.getRequiredReplicaSetName())
                .build();
    }

    private static List<MongoCredential> credentials(String dbName, String userName, String password) {
        if (userName != null && userName.trim().length() > 0) {
            return Collections.singletonList(
                    MongoCredential.createMongoCRCredential(userName, dbName, password.toCharArray()));
        } else {
            return Collections.emptyList();
        }
    }

    public WriteConcern getSafeWriteConcern() {
        return safeWriteConcern;
    }

    public void save(E item) {
        final DBObject obj = item.toDbObject();
        try {
            collection.save(obj, getSafeWriteConcern());
        } catch (MongoException e) {
            throw e;
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
            try {
                collection.remove(new BasicDBObject(), this.safeWriteConcern);
                logger.info("Cleared all entities successfully on collection: {}", collection.getFullName());
            } catch (MongoException e) {
                logger.error("Could not clear entities on collection {}: {}", collection.getFullName(), e.getMessage());
                throw e;
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
