package de.otto.jobstore.common;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.io.Serializable;

public abstract class AbstractItem<E> implements Item<E>, Serializable {

    private final DBObject dbObject;

    AbstractItem() {
        dbObject = new BasicDBObject();
    }

    AbstractItem(DBObject dbObject) {
        this.dbObject = dbObject;
    }

    @Override
    public final DBObject toDbObject() {
        return dbObject;
    }

    final void addProperty(final String key, final Object value) {
        dbObject.put(key, value);
    }

    @SuppressWarnings("unchecked")
    final <E> E getProperty(final String key) {
        return (E) dbObject.get(key);
    }

    final boolean hasProperty(final String key) {
        return dbObject.containsField(key);
    }

}
