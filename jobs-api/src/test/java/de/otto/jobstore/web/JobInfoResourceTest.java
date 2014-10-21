package de.otto.jobstore.web;

import com.mongodb.BasicDBObject;
import com.sun.jersey.api.uri.UriBuilderImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import de.otto.jobstore.common.JobExecutionPriority;
import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.RunningState;
import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.service.JobInfoService;
import de.otto.jobstore.service.JobService;
import de.otto.jobstore.service.exception.JobAlreadyQueuedException;
import de.otto.jobstore.service.exception.JobAlreadyRunningException;
import de.otto.jobstore.service.exception.JobNotRegisteredException;
import de.otto.jobstore.service.exception.JobServiceNotActiveException;
import de.otto.jobstore.web.representation.JobInfoRepresentation;
import de.otto.jobstore.web.representation.JobNameRepresentation;
import org.apache.abdera.model.Entry;
import org.apache.abdera.model.Feed;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.util.*;

import static de.otto.jobstore.TestSetup.localJobDefinition;
import static de.otto.jobstore.TestSetup.remoteJobDefinition;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

@SuppressWarnings("unchecked")
public class JobInfoResourceTest {

    private JobInfoResource jobInfoResource;
    private JobService jobService;
    private JobInfoService jobInfoService;
    private UriInfo uriInfo;
    private JobInfo JOB_INFO;

    @BeforeMethod
    public void setUp() throws Exception {
        jobService = mock(JobService.class);
        jobInfoService = mock(JobInfoService.class);
        jobInfoResource = new JobInfoResource(jobService, jobInfoService);

        uriInfo = mock(UriInfo.class);
        when(uriInfo.getBaseUriBuilder()).thenReturn(new UriBuilderImpl());
        JOB_INFO = new JobInfo(new BasicDBObject().append(JobInfoProperty.ID.val(), "1234").append(JobInfoProperty.NAME.val(), "foo"));
    }

    @Test
    public void testGetJobs() throws Exception {
        JAXBContext ctx = JAXBContext.newInstance(JobNameRepresentation.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        Set<String> names = new HashSet<>();
        names.add("foo"); names.add("bar");
        when(jobService.listJobNames()).thenReturn(names);
        Response response = jobInfoResource.getJobs(uriInfo);
        assertEquals(200, response.getStatus());
        Feed feed = (Feed) response.getEntity();

        List<Entry> entries = feed.getEntries();
        assertEquals(2, entries.size());
        Entry foo = entries.get(0);
        JobNameRepresentation fooRep = (JobNameRepresentation) unmarshaller.unmarshal(new StringReader(foo.getContent()));
        assertEquals("foo", fooRep.getName());
        Entry bar = entries.get(1);
        JobNameRepresentation barRep = (JobNameRepresentation) unmarshaller.unmarshal(new StringReader(bar.getContent()));
        assertEquals("bar", barRep.getName());
    }

    @Test
    public void testGetJobsEmpty() throws Exception {
        when(jobService.listJobNames()).thenReturn(new HashSet<String>());

        Response response = jobInfoResource.getJobs(uriInfo);
        assertEquals(200, response.getStatus());
        Feed feed = (Feed) response.getEntity();

        List<Entry> entries = feed.getEntries();
        assertEquals(0, entries.size());
    }

    @Test
    public void testExecuteJobWhichIsNotRegistered() throws Exception {
        when(jobService.executeJob(eq("foo"), eq(JobExecutionPriority.FORCE_EXECUTION), anyMap())).thenThrow(new JobNotRegisteredException(""));
        //when(jobService.executeJob("foo", false)).thenThrow(new JobNotRegisteredException(""));

        Response response = jobInfoResource.executeJob("foo", uriInfo);
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testExecuteJobWhichIsAlreadyQueued() throws Exception {
        when(jobService.executeJob(eq("foo"), eq(JobExecutionPriority.FORCE_EXECUTION), anyMap())).thenThrow(new JobAlreadyQueuedException(""));

        Response response = jobInfoResource.executeJob("foo", uriInfo);
        assertEquals(409, response.getStatus());
    }

    @Test
    public void testExecuteJobWhichIsAlreadyRunning() throws Exception {
        when(jobService.executeJob(eq("foo"), eq(JobExecutionPriority.FORCE_EXECUTION), anyMap())).thenThrow(new JobAlreadyRunningException(""));

        Response response = jobInfoResource.executeJob("foo", uriInfo);
        assertEquals(409, response.getStatus());
    }

    @Test
    public void testExecuteJobOnInactiveServiceShouldResultInBadRequestResponse() throws Exception {
        when(jobService.executeJob(eq("foo"), eq(JobExecutionPriority.FORCE_EXECUTION), anyMap())).thenThrow(new JobServiceNotActiveException("not active"));

        Response response = jobInfoResource.executeJob("foo", uriInfo);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void testExecuteJob() throws Exception {
        when(jobService.executeJob(eq("foo"), eq(JobExecutionPriority.FORCE_EXECUTION), anyMap())).thenReturn("1234");
        when(jobInfoService.getById("1234")).thenReturn(JOB_INFO);

        Response response = jobInfoResource.executeJob("foo", uriInfo);
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testGetJob() throws Exception {
        when(jobInfoService.getById("1234")).thenReturn(JOB_INFO);

        Response response = jobInfoResource.getJob("foo", "1234");
        assertEquals(200, response.getStatus());
    }

    @Test
    public void testGetJobNotExisting() throws Exception {
        when(jobInfoService.getById("1234")).thenReturn(null);

        Response response = jobInfoResource.getJob("foo", "1234");
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testGetJobMismatchingName() throws Exception {
        when(jobInfoService.getById("1234")).thenReturn(JOB_INFO);

        Response response = jobInfoResource.getJob("bar", "1234");
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testGetJobsByName() throws Exception {
        JAXBContext ctx = JAXBContext.newInstance(JobInfoRepresentation.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        when(jobInfoService.getByName("foo", 5)).thenReturn(createJobs(5, "foo"));

        Response response = jobInfoResource.getJobsByName("foo", 5, uriInfo);
        assertEquals(200, response.getStatus());
        Feed feed = (Feed) response.getEntity();

        List<Entry> entries = feed.getEntries();
        assertEquals(5, entries.size());

        Entry foo = entries.get(0);
        JobInfoRepresentation fooRep = (JobInfoRepresentation) unmarshaller.unmarshal(new StringReader(foo.getContent()));
        assertEquals("0", fooRep.getId());
        assertEquals("foo", fooRep.getName());
    }

    @Test
    public void testGetJobsByEmpty() throws Exception {
        when(jobInfoService.getByName("foo", 5)).thenReturn(new ArrayList<JobInfo>());

        Response response = jobInfoResource.getJobsByName("foo", 5, uriInfo);
        assertEquals(200, response.getStatus());
        Feed feed = (Feed) response.getEntity();

        List<Entry> entries = feed.getEntries();
        assertEquals(0, entries.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetJobHistory() throws Exception {
        Set<String> names = new HashSet<>();
        names.add("foo");
        when(jobService.listJobNames()).thenReturn(names);
        when(jobInfoService.getByNameAndTimeRange(anyString(), any(Date.class), any(Date.class), any(Set.class))).
                thenReturn(createJobs(5, "foo"));

        Response response = jobInfoResource.getJobsHistory(5, null, new HashSet(jobService.listJobNames()));
        assertEquals(200, response.getStatus());
        Map<String, List<JobInfoRepresentation>> history = (Map<String, List<JobInfoRepresentation>>) response.getEntity();
        assertEquals(1, history.size());
        assertEquals(5, history.get("foo").size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetJobHistory2() throws Exception {
        Set<String> names = new HashSet<>();
        names.add("foo");
        when(jobService.listJobNames()).thenReturn(names);
        when(jobInfoService.getByNameAndTimeRange(anyString(), any(Date.class), any(Date.class), any(Set.class))).
                thenReturn(createJobs(5, "foo"));

        Response response = jobInfoResource.getJobsHistory(5, null, null);
        assertEquals(200, response.getStatus());
        Map<String, List<JobInfoRepresentation>> history = (Map<String, List<JobInfoRepresentation>>) response.getEntity();
        assertEquals(1, history.size());
        assertEquals(0, history.get("foo").size());
    }

    @Test
    public void testStatusJob() throws Exception {
        Response response = jobInfoResource.statusOfAllJobs();
        assertEquals(200, response.getStatus());
        assertTrue(((String)response.getEntity()).contains("localRunningJobs"));
    }

    @Test
    public void testStatusWithNoRunningJobs() throws Exception {
        when(jobService.listJobNames()).thenReturn(Arrays.asList("local", "remote"));
        when(jobService.getJobDefinitionByName("local")).thenReturn(localJobDefinition("local", 10));
        when(jobService.getJobDefinitionByName("remote")).thenReturn(remoteJobDefinition("remote", 10, 10));


        Response response = jobInfoResource.statusOfAllJobs();
        assertEquals(200, response.getStatus());
        assertTrue(((String)response.getEntity()).contains("\"localRunningJobs\" : false"));
    }

    @Test
    public void testStatusWithLocalRunningJobs() throws Exception {
        when(jobService.listJobNames()).thenReturn(Arrays.asList("local", "remote"));
        when(jobService.getJobDefinitionByName("local")).thenReturn(localJobDefinition("local", 10));
        when(jobService.getJobDefinitionByName("remote")).thenReturn(remoteJobDefinition("remote", 10, 10));

        JobInfo jobInfo = mock(JobInfo.class);
        when(jobInfoService.getByNameAndRunningState("local", RunningState.RUNNING)).thenReturn(jobInfo);

        Response response = jobInfoResource.statusOfAllJobs();
        assertEquals(200, response.getStatus());
        assertTrue(((String)response.getEntity()).contains("\"localRunningJobs\" : true"));
    }

    @Test
    public void testStatusWithRemoteRunningJobs() throws Exception {
        when(jobService.listJobNames()).thenReturn(Arrays.asList("local", "remote"));
        when(jobService.getJobDefinitionByName("local")).thenReturn(localJobDefinition("local", 10));
        when(jobService.getJobDefinitionByName("remote")).thenReturn(remoteJobDefinition("remote", 10, 10));

        JobInfo jobInfo = mock(JobInfo.class);
        when(jobInfoService.getByNameAndRunningState("remote", RunningState.RUNNING)).thenReturn(jobInfo);

        Response response = jobInfoResource.statusOfAllJobs();
        assertEquals(200, response.getStatus());
        assertTrue(((String)response.getEntity()).contains("\"localRunningJobs\" : false"));
    }


    @Test
    public void testEnablingJob() throws Exception {
        Response response = jobInfoResource.enableJob("test");
        assertEquals(200, response.getStatus());
        assertTrue(((String)response.getEntity()).contains("enabled"));
    }

    @Test
    public void testDisablingJob() throws Exception {
        Response response = jobInfoResource.disableJob("test");
        assertEquals(200, response.getStatus());
        assertTrue(((String)response.getEntity()).contains("disabled"));
    }

    @Test
    public void testDisablingNotRegisteredJob() throws Exception {
        doThrow(new JobNotRegisteredException("")).when(jobService).setJobExecutionEnabled("test", false);
        Response response = jobInfoResource.disableJob("test");
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testExtractParameters() throws Exception {
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl();
        queryParameters.put("key1", Arrays.asList("v1"));
        queryParameters.put("key2", Arrays.asList("v2"));

        Map<String, String> parameters
                = jobInfoResource.extractFirstParameters(queryParameters);
        assertEquals(2, parameters.size(), 1);
        assertEquals("v1", parameters.get("key1"));
        assertEquals("v2", parameters.get("key2"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testExtractParametersFailOnNull() throws Exception {
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl();
        queryParameters.put("key1", Arrays.asList("v1"));
        queryParameters.put("key2", null);

        jobInfoResource.extractFirstParameters(queryParameters);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testExtractParametersFailOnMultipleValuesPerKey() throws Exception {
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl();
        queryParameters.put("key1", Arrays.asList("v1", "v2"));

        jobInfoResource.extractFirstParameters(queryParameters);
    }

    // ~~

    private List<JobInfo> createJobs(int number, String name) {
        List<JobInfo> jobs = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            jobs.add(new JobInfo(new BasicDBObject().append(JobInfoProperty.ID.val(), String.valueOf(i)).
                    append(JobInfoProperty.NAME.val(), name)));
        }
        return jobs;
    }

}
