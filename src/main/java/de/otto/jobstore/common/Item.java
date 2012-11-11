package de.otto.jobstore.common;

import com.mongodb.DBObject;

public interface Item<E> {

    DBObject toDbObject();

}
