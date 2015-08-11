package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.common.RemoteJobStatus;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;
import de.otto.jobstore.service.exception.RemoteJobNotRunningException;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.SocketConfig;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONException;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.Charset;

/**
 * This class triggers the execution of jobs on a remote server.
 * The scripts for execution are sent within the request body.
 *
 * The remote server has to expose a rest endpoint.
 * The multipart request sent to the endpoint consists of two parts:
 *
 * 1) A binary part containing the tar file:
 *     Content-Disposition: form-data; name="scripts"; filename="scripts.tar.gz"
 *     Content-Type: application/octet-stream
 *     Content-Transfer-Encoding: binary
 * 2) A part containing the JSON formatted parameters:
 *     Content-Disposition: form-data; name="params"
 *     Content-Type: application/json; charset=UTF-8
 *     Content-Transfer-Encoding: 8bit
 */
public class RemoteJobExecutorWithScriptTransferService implements RemoteJobExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteJobExecutorWithScriptTransferService.class);
    private final RemoteJobExecutorStatusRetriever remoteJobExecutorStatusRetriever;
    private String jobExecutorUri;
    private Client client;
    private HttpClient httpclient;
    private TarArchiveProvider tarArchiveProvider;

    @Override
    public String getJobExecutorUri() {
        return jobExecutorUri;
    }

    public RemoteJobExecutorWithScriptTransferService(String jobExecutorUri, TarArchiveProvider tarArchiveProvider) {
        this.jobExecutorUri = jobExecutorUri;
        this.tarArchiveProvider = tarArchiveProvider;

        // since Flask (with WSGI) does not suppport HTTP 1.1 chunked encoding, turn it off
        //    see: https://github.com/mitsuhiko/flask/issues/367
        final ClientConfig cc = new ClientConfig();
        cc.property(ClientProperties.CHUNKED_ENCODING_SIZE, null);
        this.client = ClientBuilder.newClient(cc);
        remoteJobExecutorStatusRetriever = new RemoteJobExecutorStatusRetriever(client);

        httpclient = createMultithreadSafeClient();

    }

    private HttpClient createMultithreadSafeClient() {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(100);
        cm.setDefaultMaxPerRoute(100);

        cm.setDefaultSocketConfig(SocketConfig.custom()
                .setSoTimeout(5000)
                .build());

        return HttpClients.custom()
                .setConnectionManager(cm)
                .build();
    }

    public URI startJob(final RemoteJob job) throws JobException {
        final String startUrl = jobExecutorUri + job.name + "/start";
        HttpResponse response = null;
        try {
            LOGGER.info("ltag=RemoteJobExecutorService.startJob Going to start job: {} ...", startUrl);

            InputStream tarInputStream = createTar(job);

            HttpPost httpPost = createRemoteExecutorMultipartRequest(job, startUrl, tarInputStream);

            response = executeRequest(httpPost);

            int statusCode = response.getStatusLine().getStatusCode();
            String link = extractLink(response);
            if (statusCode == 201) {
                return createJobUri(link);
            } else if (statusCode == 200 || statusCode == 303) {
                throw new RemoteJobAlreadyRunningException("Remote job is already running, url=" + startUrl, createJobUri(link));
            }
            throw new JobExecutionException("Unable to start remote job: url=" + startUrl + " rc=" + statusCode);
        } catch (JSONException e) {
            throw new JobExecutionException("Could not create JSON object: " + job, e);
        } catch (Exception e) {
            throw new JobExecutionException("Problem while starting new job: url=" + startUrl, e);
        } finally {
            closeResponseConnection(response);
        }
    }

    private String extractLink(HttpResponse response) {
        Header linkHeader = response.getFirstHeader("Link");
        String link;
        if (linkHeader == null) {
            link = "error";
        } else {
            link = linkHeader.getValue();
        }
        return link;
    }

    private void closeResponseConnection(HttpResponse response) {
        if (response != null) {
            try {
                EntityUtils.consume(response.getEntity());
            } catch (IOException e) {
                LOGGER.warn("Could not close response connection", e);
            }
        }
    }

    private HttpResponse executeRequest(HttpPost httpPost) throws JobExecutionException {
        HttpResponse response;
        try {
            httpPost.setConfig(RequestConfig.custom()
                    .setConnectTimeout(60000) // wait max 60 seconds
                    .build());
            response = httpclient.execute(httpPost);
        } catch (IOException e) {
            throw new JobExecutionException("Could not post scripts", e);
        }
        return response;
    }

    private InputStream createTar(RemoteJob job) throws JobExecutionException {
        try {
            return tarArchiveProvider.getArchiveAsInputStream(job);
        } catch (Exception e) {
            throw new JobExecutionException("Could not create tar with job scripts (folder: " + job.name + ")", e);
        }
    }

    public HttpPost createRemoteExecutorMultipartRequest(RemoteJob job, String startUrl, InputStream tarInputStream) throws JSONException, JobExecutionException {
        HttpPost httpPost = new HttpPost(startUrl);

        // InputStreamBody tarBody = new InputStreamBody(tarInputStream, "scripts.tar.gz");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            IOUtils.copy(tarInputStream, baos);
            tarInputStream.close();
        } catch (IOException e) {
            throw new JobExecutionException("error copying byte arrays", e);
        }
        ByteArrayBody tarBody = new ByteArrayBody(baos.toByteArray(), "scripts.tar.gz");

        MultipartEntity multipartEntity = new MultipartEntity();
        multipartEntity.addPart("scripts", tarBody);
        try {
            multipartEntity.addPart("params", new StringBody(job.toJsonObject().toString(), MediaType.APPLICATION_JSON, Charset.forName("UTF-8")));
        } catch (UnsupportedEncodingException e) {
            throw new JobExecutionException("Could not generate json", e);
        }
        httpPost.setEntity(multipartEntity);
        httpPost.setHeader("Connection", "close");
        httpPost.setHeader("User-Agent", "RemoteJobExecutorService");
        return httpPost;
    }

    public void stopJob(URI jobUri) throws JobException {
        final String stopUrl = jobUri + "/stop";
        try {
            LOGGER.info("ltag=RemoteJobExecutorService.stopJob Going to stop job: {} ...", stopUrl);
            client.target(stopUrl).request().header("Connection", "close").post(null, String.class);
        } catch (WebApplicationException e) {
            if (e.getResponse().getStatus() == 403) {
                throw new RemoteJobNotRunningException("Remote job is not running: url=" + stopUrl);
            }
            throw e;
        }
    }

    public RemoteJobStatus getStatus(final URI jobUri) {
        return remoteJobExecutorStatusRetriever.getStatus(jobUri);
    }

    public boolean isAlive() {
        return remoteJobExecutorStatusRetriever.isAlive(jobExecutorUri);
    }

    // ~

    private URI createJobUri(String path) {
        return URI.create(jobExecutorUri).resolve(path);
    }

}
