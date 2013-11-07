package de.otto.jobstore.service;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.common.RemoteJobStatus;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;
import de.otto.jobstore.service.exception.RemoteJobNotRunningException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;

public class RemoteJobExecutorWithScriptTransferService implements RemoteJobExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteJobExecutorWithScriptTransferService.class);
    private static final String JOB_SCRIPT_DIRECTORY = "/jobs";
    private final RemoteJobExecutorStatusRetriever remoteJobExecutorStatusRetriever;
    private String jobExecutorUri;
    private Client client;
    private HttpClient httpclient =  new DefaultHttpClient();
    private ScriptArchiver scriptArchiver = new ScriptArchiver();

    public RemoteJobExecutorWithScriptTransferService(String jobExecutorUri) {
        this.jobExecutorUri = jobExecutorUri;

        // since Flask (with WSGI) does not suppport HTTP 1.1 chunked encoding, turn it off
        //    see: https://github.com/mitsuhiko/flask/issues/367
        final ClientConfig cc = new DefaultClientConfig();
        cc.getProperties().put(ClientConfig.PROPERTY_CHUNKED_ENCODING_SIZE, null);
        this.client = Client.create(cc);
        remoteJobExecutorStatusRetriever = new RemoteJobExecutorStatusRetriever(client);
    }

    public URI startJob(final RemoteJob job) throws JobException {
        final String startUrl = jobExecutorUri + job.name + "/start";
        try {
            LOGGER.info("ltag=RemoteJobExecutorService.startJob Going to start job: {} ...", startUrl);

            byte[] tarAsByteArray = createTar(job);

            HttpPost httpPost = createRemoteExecutorMultipartRequest(job, startUrl, tarAsByteArray);

            HttpResponse response = executeRequest(httpPost);

            int statusCode = response.getStatusLine().getStatusCode();
            String link = response.getFirstHeader("Link").getValue();
            if (statusCode == 201) {
                return createJobUri(link);
            } else if (statusCode == 303) {
                throw new RemoteJobAlreadyRunningException("Remote job is already running, url=" + startUrl, createJobUri(link));
            }
            throw new JobExecutionException("Unable to start remote job: url=" + startUrl + " rc=" + statusCode);
        } catch (JSONException e) {
            throw new JobExecutionException("Could not create JSON object: " + job, e);
        } catch (UniformInterfaceException | ClientHandlerException  e) {
            throw new JobExecutionException("Problem while starting new job: url=" + startUrl, e);
        }
    }

    private HttpResponse executeRequest(HttpPost httpPost) throws JobExecutionException {
        HttpResponse response;
        try {
            response = httpclient.execute(httpPost);
        } catch (IOException e) {
            throw new JobExecutionException("Could not post scripts", e);
        }
        return response;
    }

    private byte[] createTar(RemoteJob job) throws JobExecutionException {
        byte[] tarAsByteArray;
        try {
            tarAsByteArray = scriptArchiver.createArchive(getJobScriptDirectory(job));
        } catch (Exception e) {
            throw new JobExecutionException("Could not create tar with job scripts (folder: " + job.name + ")", e);
        }
        return tarAsByteArray;
    }

    public HttpPost createRemoteExecutorMultipartRequest(RemoteJob job, String startUrl, byte[] tarByteArray) throws JSONException, JobExecutionException {
        HttpPost httpPost = new HttpPost(startUrl);

        ByteArrayBody tarBody = new ByteArrayBody(tarByteArray, "scripts.tar.gz");
        MultipartEntity multipartEntity = new MultipartEntity();
        multipartEntity.addPart("scripts", tarBody);
        try {
            multipartEntity.addPart("params", new StringBody(job.toJsonObject().toString()));
        } catch (UnsupportedEncodingException e) {
            throw new JobExecutionException("Could not generate json", e);
        }
        httpPost.setEntity(multipartEntity);
        return httpPost;
    }

    public void stopJob(URI jobUri) throws JobException {
        final String stopUrl = jobUri + "/stop";
        try {
            LOGGER.info("ltag=RemoteJobExecutorService.stopJob Going to stop job: {} ...", stopUrl);
            client.resource(stopUrl).header("Connection", "close").post();
        } catch (UniformInterfaceException e) {
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

    private String getJobScriptDirectory(RemoteJob job) {
        return JOB_SCRIPT_DIRECTORY + "/" + job.name;
    }

}