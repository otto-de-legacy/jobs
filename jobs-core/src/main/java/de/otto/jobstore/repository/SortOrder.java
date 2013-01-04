package de.otto.jobstore.repository;

/**
 * Enumeration for MongoDB Sort Order
 *
 * @author Sebastian Schroeder
 */
enum SortOrder {

    ASC(1),
    DESC(-1);

    private final int val;

    private SortOrder(int key) {
        this.val = key;
    }

    public int val() {
        return val;
    }

}
