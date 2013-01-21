package de.otto.jobstore.service;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
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

public class RemoteJobExecutorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteJobExecutorService.class);
    private String jobExecutorUri;
    private Client client;

    public RemoteJobExecutorService(String jobExecutorUri) {
        this.jobExecutorUri = jobExecutorUri;

        // since Flask (with WSGI) does not suppport HTTP 1.1 chunked encoding, turn it off
        //    see: https://github.com/mitsuhiko/flask/issues/367
        final ClientConfig cc = new DefaultClientConfig();
        cc.getProperties().put(ClientConfig.PROPERTY_CHUNKED_ENCODING_SIZE, null);
        this.client = Client.create(cc);
    }

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
        try {
            final ClientResponse response = client.resource(jobUri.toString()).
                    accept(MediaType.APPLICATION_JSON).header("Connection", "close").get(ClientResponse.class);
            if (response.getStatus() == 200) {
                final RemoteJobStatus status = response.getEntity(RemoteJobStatus.class);
                LOGGER.info("ltag=RemoteJobExecutorService.getStatus Response from server: {}", status);
                return status;
            } else {
                response.close();
            }
            LOGGER.warn("Received unexpected status code {} when trying to retrieve status for remote job from: {}", response.getStatus(), jobUri);
        } catch (UniformInterfaceException | ClientHandlerException e) {
            LOGGER.warn("Problem while trying to retrieve status for remote job from: {}", jobUri, e);
        }
        return null; // TODO: this should be avoided
    }

    public boolean isAlive() {
        try {
            final ClientResponse response = client.resource(jobExecutorUri).header("Connection", "close").get(ClientResponse.class);
            final boolean alive = response.getStatus() == 200;
            response.close();
            return alive;
        } catch (UniformInterfaceException | ClientHandlerException e) {
            LOGGER.warn("Remote Job Executor is not available from: {}", jobExecutorUri, e);
        }
        return false;
    }

    // ~

    private URI createJobUri(String path) {
        return URI.create(jobExecutorUri).resolve(path);
    }

}
