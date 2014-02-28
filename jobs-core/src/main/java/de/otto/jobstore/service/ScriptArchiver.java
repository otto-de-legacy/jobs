package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

/**
 * Created by ipavkovic on 24.02.14.
 */
public interface ScriptArchiver {
    InputStream createArchive(RemoteJob remoteJob) throws IOException, URISyntaxException;
}
