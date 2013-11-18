package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertTrue;

public class RemoteJobExecutorWithScriptTransferServiceTest {

    private static final String JOB_NAME = "jobname";

    private RemoteJobExecutorWithScriptTransferService remoteJobExecutorService;
    private HttpClient httpClient;

    @BeforeMethod
    public void setUp() throws Exception {
        ScriptArchiver scriptArchiver = mock(ScriptArchiver.class);
        when(scriptArchiver.createArchive(anyString(), anyString())).thenReturn(new byte[0]);
        httpClient = mock(HttpClient.class);
        remoteJobExecutorService = new RemoteJobExecutorWithScriptTransferService("uri", scriptArchiver, httpClient);
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

    @Test
    public void shouldThrowExceptionWhenReceivingUnexpectedStatus() throws Exception {
        // Given
        when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(
                createResponseWithStatusAndLink(HttpStatus.SC_NOT_FOUND, "link"));

        // When
        try {
            remoteJobExecutorService.startJob(createRemoteJob());
            fail();
        } catch (JobException e) {
        // Then
            assertTrue(e.getMessage().contains("Unable to contact remote executor"));
        }
    }

    @Test(expectedExceptions = RemoteJobAlreadyRunningException.class)
    public void shouldThrowExceptionWhenJobIsAlreadyRunning() throws Exception {
        // Given
        BasicHttpResponse response = createResponseWithStatusAndLink(HttpStatus.SC_SEE_OTHER, "http://server/path");
        when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(
                response);

        // When
        remoteJobExecutorService.startJob(createRemoteJob());
    }

    @Test
    public void shouldCreateURIFromResponseHeader() throws Exception {
        // Given
        BasicHttpResponse response = createResponseWithStatusAndLink(HttpStatus.SC_CREATED, "http://server/path");
        when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(
                response);

        // When
        URI uri = remoteJobExecutorService.startJob(createRemoteJob());
        // Then
        assertTrue(uri.toString().contains("http://server/path"));
    }

    private BasicHttpResponse createResponseWithStatusAndLink(int statusCode, String link) {
        BasicHttpResponse response = new BasicHttpResponse(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 0), statusCode, "reason"));
        response.addHeader("Link", link);
        return response;
    }

    private RemoteJob createRemoteJob() {
        Map<String, String> params = new HashMap();
        return new RemoteJob(JOB_NAME, "2311", params);
    }

}
