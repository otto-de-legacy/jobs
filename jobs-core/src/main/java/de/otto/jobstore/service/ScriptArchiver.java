package de.otto.jobstore.service;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class ScriptArchiver {

    public byte[] createArchive(String directory) throws IOException {
        TarArchiveOutputStream tarArchive = null;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            tarArchive = new TarArchiveOutputStream(
                    new GZIPOutputStream(
                            new BufferedOutputStream(byteArrayOutputStream)));

            File[] files = new File(directory).listFiles();
            for (File file : files) {
                TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(file);
                tarArchive.putArchiveEntry(tarArchiveEntry);
                tarArchive.closeArchiveEntry();
            }
        } finally {
            if (tarArchive != null) tarArchive.close();
        }

        return byteArrayOutputStream.toByteArray();
    }
}
