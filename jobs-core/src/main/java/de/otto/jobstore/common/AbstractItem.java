package de.otto.jobstore.common;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.otto.jobstore.common.properties.ItemProperty;

import java.io.Serializable;

/**
 *  Abstract Class for Objects to be stored in MongoDB
 */
public abstract class AbstractItem implements Item, Serializable {

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

    final void addProperty(final ItemProperty key, final Object value) {
        dbObject.put(key.val(), value);
    }

    @SuppressWarnings("unchecked")
    final <E> E getProperty(final ItemProperty key) {
        return (E) dbObject.get(key.val());
    }

    final boolean hasProperty(final ItemProperty key) {
        return dbObject.containsField(key.val());
    }

}
