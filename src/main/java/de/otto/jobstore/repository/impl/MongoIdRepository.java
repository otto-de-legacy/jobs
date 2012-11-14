package de.otto.jobstore.repository.impl;

import com.mongodb.*;
import de.otto.jobstore.repository.api.IdRepository;

public final class MongoIdRepository extends AbstractMongoRepository implements IdRepository {

    public MongoIdRepository(Mongo mongo, String dbName, String collectionName) {
        super(mongo, dbName, collectionName);
    }

    public MongoIdRepository(Mongo mongo, String dbName, String collectionName, String username, String password) {
        super(mongo, dbName, collectionName, username, password);
    }

    @Override
    public Long getId(final String collectionName) {
        final DBObject query = new BasicDBObject("_id", collectionName);
        final DBObject update = new BasicDBObject("$inc", new BasicDBObject("value", 1));
        final DBObject id = collection.findAndModify(query, update);
        if (id == null) {
            collection.save(new BasicDBObjectBuilder().append("_id", collectionName)
                    .append("value", 1L).get());
            return 0L;
        } else {
            return (Long) id.get("value");
        }
    }

    protected void prepareCollection() {}

}
