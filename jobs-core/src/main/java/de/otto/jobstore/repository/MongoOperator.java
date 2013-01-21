package de.otto.jobstore.repository;

/**
 * Enumeration of MongoDB Query Operators
 *
 * @author Sebastian Schroeder
 */
enum MongoOperator {

    GTE("$gte"),
    IN("$in"),
    LT("$lt"),
    LTE("$lte"),
    NE("$ne"),
    NIN("$nin"),
    PUSH("$push"),
    PUSH_ALL("$pushAll"),
    SET("$set");

    private final String op;

    private MongoOperator(final String op) {
        this.op = op;
    }

    public String op() {
        return op;
    }

}
