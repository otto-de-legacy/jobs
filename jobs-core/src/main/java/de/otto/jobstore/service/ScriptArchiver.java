package de.otto.jobstore.service;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

public class ScriptArchiver {

    public byte[] createArchive(String directory) throws IOException, URISyntaxException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (TarArchiveOutputStream tarArchive = new TarArchiveOutputStream(
                            new GZIPOutputStream(
                                    new BufferedOutputStream(byteArrayOutputStream)))){

            for (File file: getResources(directory)) {
                writeEntry(tarArchive, file);
            }
        }

        return byteArrayOutputStream.toByteArray();
    }

    private void writeEntry(TarArchiveOutputStream tarArchive, File file) throws IOException {
        try (InputStream fis = new FileInputStream(file)) {
            TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(file.getName());
            tarArchiveEntry.setSize(file.length());
            tarArchive.putArchiveEntry(tarArchiveEntry);
            IOUtils.copy(fis, tarArchive);
            tarArchive.closeArchiveEntry();
        }
    }

    public File[] getResources(String path) throws URISyntaxException, IOException {
        URL dirURL = getClass().getResource(path);
        if (dirURL == null) {
            throw new IOException("Could not find directory \"" + path + "\"");
        }
        File file = new File(dirURL.toURI());
        return file.listFiles();
    }
}
