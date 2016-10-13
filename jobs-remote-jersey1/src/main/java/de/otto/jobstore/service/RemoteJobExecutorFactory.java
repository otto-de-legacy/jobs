package de.otto.jobstore.service;

@SuppressWarnings("unused") // official usage interface
public class RemoteJobExecutorFactory {
    private static final int CONNECTION_TIMEOUT = 5000; // wait max 5 seconds;
    private static final int READ_TIMEOUT = 20000;

    public static RemoteJobExecutor create(String jobExecutorUri, TarArchiveProvider tarArchiveProvider) {
        return create(jobExecutorUri, tarArchiveProvider, CONNECTION_TIMEOUT, READ_TIMEOUT);
    }

    public static RemoteJobExecutor create(String jobExecutorUri, TarArchiveProvider tarArchiveProvider, int connectionTimeout, int socketTimeout) {
        return RemoteJobExecutorWithScriptTransferService.create(create(jobExecutorUri, connectionTimeout, socketTimeout), jobExecutorUri, tarArchiveProvider);
    }

    public static RemoteJobExecutor create(String jobExecutorUri) {
        return create(jobExecutorUri, CONNECTION_TIMEOUT, READ_TIMEOUT);
    }

    public static RemoteJobExecutor create(String jobExecutorUri, int connectionTimeout, int readTimeout) {
        return new RemoteJobExecutorService(jobExecutorUri, connectionTimeout, readTimeout);
    }
}
