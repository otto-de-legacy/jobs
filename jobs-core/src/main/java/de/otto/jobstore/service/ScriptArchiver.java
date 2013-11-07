package de.otto.jobstore.service;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

public class ScriptArchiver {

    public byte[] createArchive(String directory) throws IOException, URISyntaxException {
        TarArchiveOutputStream tarArchive = null;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            tarArchive = new TarArchiveOutputStream(
                    new GZIPOutputStream(
                            new BufferedOutputStream(byteArrayOutputStream)));

            String[] fileNames = getResources(directory);
            for (String fileName : fileNames) {
                TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(new File(fileName));
                tarArchive.putArchiveEntry(tarArchiveEntry);
                tarArchive.closeArchiveEntry();
            }
        } finally {
            if (tarArchive != null) tarArchive.close();
        }

        return byteArrayOutputStream.toByteArray();
    }

    public String[] getResources(String path) throws URISyntaxException, IOException {
        URL dirURL = getClass().getResource(path);
        if (dirURL==null) {
            throw new IOException("Could not find directory \"" + path + "\"");
        }
        File file = new File(dirURL.toURI());
        return file.list();
    }
}
