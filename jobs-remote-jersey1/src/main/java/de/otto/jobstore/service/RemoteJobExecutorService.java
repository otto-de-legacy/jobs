package de.otto.jobstore.service;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.common.RemoteJobStatus;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;
import de.otto.jobstore.service.exception.RemoteJobNotRunningException;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
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
            final ClientResponse response = client.resource(startUrl)
                    .type(MediaType.APPLICATION_JSON).header("Connection", "close").header("User-Agent", "RemoteJobExecutorService")
                    .post(ClientResponse.class, job.toJsonObject());
            if (response.getStatus() == 201) {
                return createJobUri(response.getHeaders().getFirst("Link"));
            } else if (response.getStatus() == 303) {
                throw new RemoteJobAlreadyRunningException("Remote job is already running, url=" + startUrl, createJobUri(response.getHeaders().getFirst("Link")));
            }
            throw new JobExecutionException("Unable to start remote job: url=" + startUrl + " rc=" + response.getStatus());
        } catch (JSONException e) {
            throw new JobExecutionException("Could not create JSON object: " + job, e);
        } catch (UniformInterfaceException | ClientHandlerException  e) {
            throw new JobExecutionException("Problem while starting new job: url=" + startUrl, e);
        }
    }

    @Override
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

}
