package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.common.RemoteJobStatus;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;
import de.otto.jobstore.service.exception.RemoteJobNotRunningException;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;

import static de.otto.jobstore.service.RemoteJobExecutorWithScriptTransferService.createClient;

public class RemoteJobExecutorService implements RemoteJobExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteJobExecutorService.class);
    private final RemoteJobExecutorStatusRetriever remoteJobExecutorStatusRetriever;

    private String jobExecutorUri;
    private Client client;

    public RemoteJobExecutorService(String jobExecutorUri, Client client) {
        this.jobExecutorUri = jobExecutorUri;
        this.client = client;
        this.remoteJobExecutorStatusRetriever = new RemoteJobExecutorStatusRetriever(client);
    }

    public RemoteJobExecutorService(String jobExecutorUri) {
        this(jobExecutorUri, createClient());
    }

    @Override
    public String getJobExecutorUri() {
        return jobExecutorUri;
    }

    @Override
    public URI startJob(final RemoteJob job) throws JobException {
        final String startUrl = jobExecutorUri + job.name + "/start";
        try {
            LOGGER.info("ltag=RemoteJobExecutorService.startJob Going to start job: {} ...", startUrl);
            final Response response = client.target(startUrl).request()
                    .accept(MediaType.APPLICATION_JSON).header("Connection", "close").header("User-Agent", "RemoteJobExecutorService")
                    .post(Entity.entity(job.toJsonObject(), MediaType.APPLICATION_JSON_TYPE), Response.class);
            if (response.getStatus() == 201) {
                return createJobUri(response.getHeaderString("Link"));
            } else if (response.getStatus() == 303) {
                throw new RemoteJobAlreadyRunningException("Remote job is already running, url=" + startUrl, createJobUri(response.getHeaderString("Link")));
            }
            throw new JobExecutionException("Unable to start remote job: url=" + startUrl + " rc=" + response.getStatus());
        } catch (JSONException e) {
            throw new JobExecutionException("Could not create JSON object: " + job, e);
        } catch (ProcessingException | WebApplicationException e) {
            throw new JobExecutionException("Problem while starting new job: url=" + startUrl, e);
        }
    }

    @Override
    public void stopJob(URI jobUri) throws JobException {
        final String stopUrl = jobUri + "/stop";
        try {
            LOGGER.info("ltag=RemoteJobExecutorService.stopJob Going to stop job: {} ...", stopUrl);
            client.target(stopUrl).request().header("Connection", "close").post(null, String.class);
        } catch (WebApplicationException e) {
            if (e.getResponse().getStatus() == 403) { // TODO might be 404 ?!
                throw new RemoteJobNotRunningException("Remote job is not running: url=" + stopUrl, e);
            }
            throw e;
        } catch (ProcessingException e) {
            throw new RemoteJobNotRunningException("Remote job is not running: url=" + stopUrl, e);
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
