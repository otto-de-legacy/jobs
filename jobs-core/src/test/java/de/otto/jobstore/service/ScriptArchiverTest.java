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

        String jobDirectory = "/jobs";
        String jobName = "jobname_env";
        byte[] output = scriptArchiver.createArchive(jobDirectory, jobName);

//        OutputStream outputStream = new FileOutputStream("/tmp/test.tar.gz");
//        outputStream.write(output);
//        outputStream.close();

        assertArchiveContainsExecutableFiles(output, "demoscript.sh", "env.conf");
    }

    @Test
    public void shouldTarFilesForJobNameWithoutEnvironmentPrefix() throws Exception {
        ScriptArchiver scriptArchiver = new ScriptArchiver();

        String jobDirectory = "/jobs";
        String jobName = "jobWithNoEnv";
        byte[] output = scriptArchiver.createArchive(jobDirectory, jobName);

        assertArchiveContainsExecutableFiles(output, "demoscript.sh");
    }

    @Test
    public void shouldTarFilesForJobNameWithEnvironmentPrefixButNoEnvironmentFolder() throws Exception {
        ScriptArchiver scriptArchiver = new ScriptArchiver();

        String jobDirectory = "/jobs";
        String jobName = "jobWithEnvButNoEnvFolder_environment";
        byte[] output = scriptArchiver.createArchive(jobDirectory, jobName);

        assertArchiveContainsExecutableFiles(output, "demoscript.sh");
    }

    private void assertArchiveContainsExecutableFiles(byte[] output, String... files) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(output);
        TarArchiveInputStream tarInput =  new TarArchiveInputStream(new GZIPInputStream(byteArrayInputStream));

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
