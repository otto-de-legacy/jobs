package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

/**
 *
 */
public interface TarArchiveProvider {
    /**
     * returns an InputStream on the tar archive for the given job. This shall contain all required files
     * for remote execution
     * @param remoteJob
     * @return
     * @throws IOException
     *
     */
    InputStream getArchiveAsInputStream(RemoteJob remoteJob) throws IOException;
}
