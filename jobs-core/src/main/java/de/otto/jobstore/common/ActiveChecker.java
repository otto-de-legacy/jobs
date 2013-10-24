package de.otto.jobstore.common;

/**
 * This Interface has the purpose to check, if the server is currently active and should execute jobs.
 *
 * Everytime a job should be executed (Either directly or by executing a queued job) this interface gets called.
 *
 * Usages may be a green-blue deployment, where only the newly deployed servers should execute jobs.
 * Or if you divide your servers in online and batch servers with the same database, but only the batch-server should execute jobs.
 */
public interface ActiveChecker {

    boolean isActive();

}
