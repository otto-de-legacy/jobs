package de.otto.jobstore.service.impl;


import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import de.otto.jobstore.common.RemoteJob;
import de.otto.jobstore.common.RemoteJobStatus;
import de.otto.jobstore.service.api.RemoteJobExecutorService;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobExecutionException;
import de.otto.jobstore.service.exception.RemoteJobAlreadyRunningException;
import de.otto.jobstore.service.exception.RemoteJobNotRunningException;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.net.URI;

public final class RemoteJobExecutorServiceImpl implements RemoteJobExecutorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteJobExecutorServiceImpl.class);
    private String jobExecutorUri;
    private Client client;

    public RemoteJobExecutorServiceImpl(String jobExecutorUri, Client client) {
        this.jobExecutorUri = jobExecutorUri;
        this.client = client;
    }

    @Override
    public URI startJob(final RemoteJob job) throws JobException {
        try {
            final ClientResponse response = client.resource(jobExecutorUri + job.name + "/start").
                    type(MediaType.APPLICATION_JSON).post(ClientResponse.class, job.toJsonObject());
            if (response.getStatus() == 201) {
                return createJobUri(response.getHeaders().getFirst("Link"));
            } else if (response.getStatus() == 303) {
                throw new RemoteJobAlreadyRunningException("Remote job is already running", createJobUri(response.getHeaders().getFirst("Link")));
            }
            throw new JobExecutionException("Received unexpected status code " + response.getStatus() + " when trying to create remote job " + job.name);
        } catch (JSONException e) {
            throw new JobExecutionException("Could not create json object from remote job object", e);
        } catch (UniformInterfaceException | ClientHandlerException  e) {
            throw new JobExecutionException("Received unexpected exception when trying to create remote job " + job.name, e);
        }
    }

    @Override
    public void stopJob(URI jobUri) throws JobException {
        try {
            client.resource(jobUri + "/stop").post();
        } catch (UniformInterfaceException e) {
            if (e.getResponse().getStatus() == 403) {
                throw new RemoteJobNotRunningException("Remote job '" + jobUri.toString() + "' is not running");
            }
            throw e;
        }
    }

    @Override
    public RemoteJobStatus getStatus(final URI jobUri) {
        try {
            final ClientResponse response = client.resource(jobUri.toString()).
                    accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
            if (response.getStatus() == 200) {
                return response.getEntity(RemoteJobStatus.class);
            }
            LOGGER.warn("Received unexpected status code {} when trying to retrieve status for remote job from: {}", response.getStatus(), jobUri);
        } catch (UniformInterfaceException | ClientHandlerException e) {
            LOGGER.warn("Received unexpected exception when trying to retrieve status for remote job from: {}", jobUri, e);
        }
        return null;
    }

    private URI createJobUri(String path) {
        return URI.create(jobExecutorUri).resolve(path);
    }

}
