package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class DirectoryBasedTarArchiveProviderTest {

    @Test
    public void shouldTarFilesForJobName() throws Exception {
        TarArchiveProvider tarArchiveProvider = new DirectoryBasedTarArchiveProvider("/jobs");

        RemoteJob remoteJob = new RemoteJob("demojob1", "client_id", new HashMap<String, String>());
        InputStream inputStream = tarArchiveProvider.getArchiveAsInputStream(remoteJob);

        assertArchiveContainsExecutableFiles(inputStream, "demoscript.sh", "demojob1.conf");
    }

    private void assertArchiveContainsExecutableFiles(InputStream inputStream, String... files) throws IOException {
        TarArchiveInputStream tarInput =  new TarArchiveInputStream(new GZIPInputStream(inputStream));

        TarArchiveEntry tarEntry = tarInput.getNextTarEntry();
        List<String> fileNames = new ArrayList();
        while(tarEntry != null) {
            fileNames.add(tarEntry.getName());
            assertEquals(tarEntry.getMode(), 0100755);
            tarEntry = tarInput.getNextTarEntry();
        }
        assertEquals(fileNames.size(), files.length);

        for (String filename : files) {
            assertTrue(fileNames.contains(filename));
        }
    }
}