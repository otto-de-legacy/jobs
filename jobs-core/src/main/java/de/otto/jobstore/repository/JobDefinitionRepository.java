package de.otto.jobstore.repository;

import com.mongodb.*;
import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobDefinitionProperty;


public class JobDefinitionRepository extends AbstractRepository<StoredJobDefinition> {

    public JobDefinitionRepository(Mongo mongo, String dbName, String collectionName) {
        super(mongo, dbName, collectionName);
    }

    public JobDefinitionRepository(Mongo mongo, String dbName, String collectionName, String username, String password) {
        super(mongo, dbName, collectionName, username, password);
    }

    public StoredJobDefinition find(String name) {
        final DBObject object = collection.findOne(new BasicDBObject(JobDefinitionProperty.NAME.val(), name));
        return fromDbObject(object);
    }

    @Override
    protected void prepareCollection() {
        collection.ensureIndex(new BasicDBObject(JobDefinitionProperty.NAME.val(), 1), "name", true);
    }

    @Override
    protected StoredJobDefinition fromDbObject(DBObject dbObject) {
        if (dbObject == null) {
            return null;
        }
        return new StoredJobDefinition(dbObject);
    }

    public void addOrUpdate(StoredJobDefinition jobDefinition) {
        final DBObject obj = new BasicDBObject(MongoOperator.SET.op(), buildUpdateObject(jobDefinition));
        collection.update(new BasicDBObject(JobDefinitionProperty.NAME.val(), jobDefinition.getName()), obj, true, false, WriteConcern.SAFE);
    }

    private BasicDBObject buildUpdateObject(StoredJobDefinition jobDefinition) {
        final BasicDBObject basicDBObject = new BasicDBObject();
        final DBObject jobDefObj = jobDefinition.toDbObject();
        for (JobDefinitionProperty property : JobDefinitionProperty.values()) {
            if (!property.isDynamic()) {
                basicDBObject.append(property.val(), jobDefObj.get(property.val()));
            }
        }
        return basicDBObject;
    }

    public void setJobExecutionEnabled(String name, boolean executionEnabled) {
        collection.update(new BasicDBObject(JobDefinitionProperty.NAME.val(), name),
                new BasicDBObject(MongoOperator.SET.op(), new BasicDBObject(JobDefinitionProperty.DISABLED.val(), !executionEnabled)));
    }

}
