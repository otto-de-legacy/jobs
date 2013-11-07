package de.otto.jobstore.service;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ScriptArchiverTest {

    @Test
    public void shouldTarFilesForJobName() throws Exception {
        ScriptArchiver scriptArchiver = new ScriptArchiver();

        String directoryName = "/jobs/demojob";
        byte[] output = scriptArchiver.createArchive(directoryName);

        assertArchiveContains(output, "demoscript.sh");
    }

    @Test(expectedExceptions = IOException.class)
    public void shouldThrowIOExceptionWhenFolderNotPresent() throws Exception {
        ScriptArchiver scriptArchiver = new ScriptArchiver();
        scriptArchiver.createArchive("/not_present");
    }

    private void assertArchiveContains(byte[] output, String... files) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(output);
        TarArchiveInputStream tarInput =  new TarArchiveInputStream(new GZIPInputStream(byteArrayInputStream));

        TarArchiveEntry tarEntry = tarInput.getNextTarEntry();
        List<String> fileNames = new ArrayList();
        while(tarEntry != null) {
            fileNames.add(tarEntry.getName());
            tarEntry = tarInput.getNextTarEntry();
        }
        assertEquals(fileNames.size(), files.length);

        for (String filename : files) {
            assertTrue(fileNames.contains(filename));
        }
    }

}
