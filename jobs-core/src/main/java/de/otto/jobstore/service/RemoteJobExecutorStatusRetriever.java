package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;

public class RemoteJobExecutorStatusRetriever {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteJobExecutorStatusRetriever.class);
    private final Client client;

    public RemoteJobExecutorStatusRetriever(Client client) {
        this.client = client;
    }

    public RemoteJobStatus getStatus(final URI jobUri) {
        try {
            final Response response = client.target(jobUri.toString()).request().
                    accept(MediaType.APPLICATION_JSON).
                    header("Connection", "close").
                    get(Response.class);
            if (response.getStatus() == 200) {
                final RemoteJobStatus status = response.readEntity(RemoteJobStatus.class);
                LOGGER.info("ltag=RemoteJobExecutorService.getStatus Response from server: {}", status);
                return status;
            } else {
                response.close();
            }
            LOGGER.warn("Received unexpected status code {} when trying to retrieve status for remote job from: {}", response.getStatus(), jobUri);
        } catch (ProcessingException| IllegalStateException e) {
            LOGGER.warn("Problem while trying to retrieve status for remote job from: {}", jobUri, e);
        }
        return null; // TODO: this should be avoided
    }

    public boolean isAlive(String jobExecutorUri) {
        try {
            final Response response = client.target(jobExecutorUri).
                    request().header("Connection", "close").
                    get(Response.class);
            final boolean alive = response.getStatus() == 200;
            response.close();
            return alive;
        } catch (Exception e) {
            LOGGER.warn("Remote Job Executor is not available from: {}", jobExecutorUri, e);
        }
        return false;
    }

}
