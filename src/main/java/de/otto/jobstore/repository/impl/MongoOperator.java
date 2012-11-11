package de.otto.jobstore.repository.impl;

/**
 * Enumeration of MongoDB Query Operators
 *
 * @author Sebastian Schroeder
 */
enum MongoOperator {

    GTE("$gte"),
    //IN("$in"),
    //INC("$inc"),
    LT("$lt"),
    LTE("$lte"),
    NE("$ne"),
    NIN("$nin"),
    //OR("$or"),
    PUSH("$push"),
    //RENAME("$rename"),
    SET("$set");
    //UNSET("$unset");

    private final String op;

    private MongoOperator(final String op) {
        this.op = op;
    }

    public String op() {
        return op;
    }
}
