package de.otto.jobstore.service;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import de.otto.jobstore.common.RemoteJobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.net.URI;

class RemoteJobExecutorStatusRetriever {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteJobExecutorStatusRetriever.class);
    private final Client client;

    public RemoteJobExecutorStatusRetriever(Client client) {
        this.client = client;
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

    public boolean isAlive(String jobExecutorUri) {
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

}
