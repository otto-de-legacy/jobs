package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * The script archiver generates a tar with all needed artifacts (scripts, config files...)
 * <p/>
 * It is passed the baseDirectory where all jobs reside. Every job has a matching subdirectory in the jobs folder.
 * <p/>
 * The archiver puts all the "global" files of the specific job folder into the tar.
 * <p/>
 * Example:
 * <p/>
 * /jobs
 * /jobname1
 * /jobname2
 * <p/>
 *
 */
public class DirectoryBasedTarArchiveProvider implements TarArchiveProvider {

    private static final int FOR_ALL_EXECUTABLE_FILE = 0100755;

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryBasedTarArchiveProvider.class);
    private String baseDirectory;

    public DirectoryBasedTarArchiveProvider(String baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    @Override
    public InputStream getArchiveAsInputStream(RemoteJob remoteJob) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (TarArchiveOutputStream tarArchive = new TarArchiveOutputStream(
                new GZIPOutputStream(
                        new BufferedOutputStream(byteArrayOutputStream)))) {

            for (String givenDirectory : getTarInputDirectories(remoteJob)) {
                writeEntriesForDirectory(givenDirectory, tarArchive);
            }
        }
        return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    }

    protected List<String> getTarInputDirectories(RemoteJob remoteJob) {
        return Collections.singletonList(getBaseDirectory() + File.separator + remoteJob.name);
    }

    protected String getBaseDirectory() {
        return baseDirectory;
    }

    private void writeEntriesForDirectory(String givenDirectory, TarArchiveOutputStream tarArchive) throws IOException {
        for (File file : getResources(givenDirectory)) {
            if (file.isFile()) {
                writeEntry(tarArchive, file);
            }
        }
    }

    private void writeEntry(TarArchiveOutputStream tarArchive, File file) throws IOException {
        try (InputStream fis = new FileInputStream(file)) {
            TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(file.getName());
            tarArchiveEntry.setSize(file.length());
            tarArchiveEntry.setMode(FOR_ALL_EXECUTABLE_FILE);
            tarArchive.putArchiveEntry(tarArchiveEntry);
            IOUtils.copy(fis, tarArchive);
            tarArchive.closeArchiveEntry();
        }
    }

    private File[] getResources(String path) throws IOException {
        File[] result;
        URL dirURL = getClass().getResource(path);
        if (dirURL == null) {
            LOGGER.info("Could not find baseDirectory \"" + path + "\"");
            result = new File[0];
        } else {
            try {
                File directory = new File(dirURL.toURI());
                result = directory.listFiles();
            } catch(URISyntaxException e) {
                throw new IOException(e);
            }
        }
        return result;
    }

}
