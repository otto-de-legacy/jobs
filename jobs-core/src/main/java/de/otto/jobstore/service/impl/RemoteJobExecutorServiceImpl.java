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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public URI startJob(RemoteJob job) throws JobException {
        try {
            final ClientResponse response = client.resource(jobExecutorUri + job.getName()).post(ClientResponse.class, job);
            if (response.getStatus() == 201) {
                return URI.create(response.getHeaders().getFirst("Link"));
            }
            throw new JobExecutionException("Received unexpected status code" + response.getStatus() + "when trying to create remote job " + job.getName());
        } catch (UniformInterfaceException e) {
            final ClientResponse response = e.getResponse();
            if (response.getStatus() == 303) {
                throw new RemoteJobAlreadyRunningException("Remote job is already running", URI.create(response.getHeaders().getFirst("Link")));
            } else {
                throw new JobExecutionException("Received unexpected status code" + response.getStatus() + "when trying to create remote job " + job.getName());
            }
        }
    }

    @Override
    public RemoteJobStatus getStatus(URI jobUri) {
        try {
            final ClientResponse response = client.resource(jobUri.toString()).get(ClientResponse.class);
            if (response.getStatus() == 200) {
                return response.getEntity(RemoteJobStatus.class);
            }
            LOGGER.warn("Received unexpected status code {} when trying to retrieve status for remote job from: {}", response.getStatus(), jobUri);
        } catch (UniformInterfaceException e) {
            LOGGER.warn("Received unexpected status code {} when trying to retrieve status for remote job from: {}", e.getResponse().getStatus(), jobUri);
        } catch (ClientHandlerException e) {
            LOGGER.error("Received exception while trying to retrieve remote job status", e);
        }
        return null;
    }

}
