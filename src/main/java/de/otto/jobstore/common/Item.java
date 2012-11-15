package de.otto.jobstore.common;

import com.mongodb.DBObject;

/**
 * Item to be stored in MongoDb
 */
public interface Item {

    /**
     * Converts an Item to a DBObject
     *
     * @return The DBObject created from the item
     */
    DBObject toDbObject();

}
