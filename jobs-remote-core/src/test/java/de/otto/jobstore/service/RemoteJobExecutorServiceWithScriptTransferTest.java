package de.otto.jobstore.service;

import de.otto.jobstore.common.RemoteJob;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;

import static org.mockito.Mockito.*;

public class RemoteJobExecutorServiceWithScriptTransferTest {

    private RemoteJobExecutorWithScriptTransferService remoteJobExecutorService;
    private RemoteJobStarter remoteJobStarter;
    private RemoteJobExecutor delegate;

    @BeforeMethod
    public void setUp() {
        remoteJobStarter = mock(RemoteJobStarter.class);
        delegate = mock(RemoteJobExecutor.class);
        remoteJobExecutorService = new RemoteJobExecutorWithScriptTransferService(delegate, remoteJobStarter);
    }

    @Test
    public void shouldCallRemoteJobStarterOnStart() throws Exception {
        // given
        RemoteJob remoteJob = mock(RemoteJob.class);

        // when
        remoteJobExecutorService.startJob(remoteJob);

        // then
        verify(remoteJobStarter).startJob(remoteJob);
        verifyZeroInteractions(delegate);
    }


    @Test
    public void shouldCallDelegateOnStop() throws Exception {
        // given
        URI uri = URI.create("http://localhost");

        // when
        remoteJobExecutorService.stopJob(uri);

        // then
        verify(delegate).stopJob(uri);
        verifyZeroInteractions(remoteJobStarter);
    }

}
