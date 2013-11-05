package de.otto.jobstore.service;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ScriptArchiverTest {

    private String separator = System.getProperty("file.separator");

    @Test
    public void shouldTarFilesForJobName() throws Exception {
        // Given
        ScriptArchiver scriptArchiver = new ScriptArchiver();

        Path directory = Files.createTempDirectory("jobname");
        new File(directory.toString()).deleteOnExit();
        String directoryName = directory.toFile().getAbsolutePath();
        touchFile(directoryName, "script1");
        touchFile(directoryName, "script2");

        // When
        byte[] output = scriptArchiver.createArchive(directoryName);

        // Then
        assertArchiveContains(directoryName, output, "script1", "script2");
    }

    private void assertArchiveContains(String directoryName, byte[] output, String... files) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(output);
        TarArchiveInputStream tarInput =  new TarArchiveInputStream(new GZIPInputStream(byteArrayInputStream));

        TarArchiveEntry tarEntry = tarInput.getNextTarEntry();
        List<String> filenames = new ArrayList();
        while(tarEntry != null) {
            filenames.add(tarEntry.getName());
            tarEntry = tarInput.getNextTarEntry();
        }
        assertEquals(filenames.size(), files.length);

        for (String filename : files) {
            assertTrue(filenames.contains(directoryName.substring(1) + separator + filename));
        }
    }

    private void touchFile(String directoryName, String filename) throws IOException {
        File file = new File(directoryName, filename);
        file.createNewFile();
        file.deleteOnExit();
    }
}
