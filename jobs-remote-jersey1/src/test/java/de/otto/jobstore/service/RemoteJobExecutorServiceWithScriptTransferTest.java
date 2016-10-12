package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.testng.AssertJUnit.assertTrue;

public class RemoteJobExecutorServiceWithScriptTransferTest {

    private static final String JOB_NAME = "jobname";
    private static final String JOB_SCRIPT_DIRECTORY = "/jobs";

    private RemoteJobExecutorWithScriptTransferService remoteJobExecutorService;

    @BeforeMethod
    public void setUp() {
        remoteJobExecutorService = new RemoteJobExecutorWithScriptTransferService("uri", new DirectoryBasedTarArchiveProvider(JOB_SCRIPT_DIRECTORY));
    }

    @Test
    public void shouldCreateMultipartRequestWithScriptsAndParams() throws Exception {
        // When
        InputStream tarAsByteArray = new ByteArrayInputStream(new byte[0]);
        HttpPost request = remoteJobExecutorService.createRemoteExecutorMultipartRequest(createRemoteJob(), "url", tarAsByteArray);
        // Then
        HttpEntity multipartEntity = request.getEntity();
        OutputStream os = new ByteArrayOutputStream();
        multipartEntity.writeTo(os);
        String requestAsString = os.toString();
        assertTrue(requestAsString.contains("name=\"params\""));
        assertTrue(requestAsString.contains("filename=\"scripts.tar.gz\""));
    }

    private RemoteJob createRemoteJob() {
        Map<String, String> params = new HashMap();
        return new RemoteJob(JOB_NAME, "2311", params);
    }

}
