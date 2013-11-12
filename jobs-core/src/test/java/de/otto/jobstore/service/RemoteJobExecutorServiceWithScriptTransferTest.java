package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.testng.AssertJUnit.assertTrue;

public class RemoteJobExecutorServiceWithScriptTransferTest  {

    private static final String JOB_NAME = "jobname";

    private RemoteJobExecutorWithScriptTransferService remoteJobExecutorService;

    @BeforeMethod
    public void setUp() {
        remoteJobExecutorService = new RemoteJobExecutorWithScriptTransferService("uri");
    }

    @Test
    public void shouldCreateMultipartRequestWithScriptsAndParams() throws Exception {
        // When
        byte[] tarAsByteArray = new byte[0];
        HttpPost request = remoteJobExecutorService.createRemoteExecutorMultipartRequest(createRemoteJob(), "url", tarAsByteArray);
        // Then
        MultipartEntity multipartEntity = (MultipartEntity) request.getEntity();
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
