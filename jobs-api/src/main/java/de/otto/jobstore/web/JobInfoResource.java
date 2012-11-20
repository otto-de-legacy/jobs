package de.otto.jobstore.web;

import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.repository.api.JobInfoRepository;
import de.otto.jobstore.service.api.JobService;
import de.otto.jobstore.service.exception.JobException;
import de.otto.jobstore.service.exception.JobNotRegisteredException;
import de.otto.jobstore.web.representation.JobInfoRepresentation;
import de.otto.jobstore.web.representation.JobsListRepresentation;
import de.otto.jobstore.web.representation.NameLinkRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.*;


/**
 *
 */
@Path("/jobs")
public final class JobInfoResource {

    public static final String OTTO_JOBS_XML = "application/vnd.otto.jobs+xml";
    public static final String OTTO_JOBS_JSON = "application/vnd.otto.jobs+json";
    private static final Logger LOGGER = LoggerFactory.getLogger(JobInfoResource.class);

    private final JobInfoRepository jobInfoRepository;

    private final JobService jobService;

    public JobInfoResource(JobInfoRepository jobInfoRepository, JobService jobService) {
        this.jobInfoRepository = jobInfoRepository;
        this.jobService = jobService;
    }

    @GET
    //@Produces({MediaType.OTTO_JOBS_XML, MediaType.OTTO_JOBS_JSON})
    public Response getJobs(@Context final UriInfo uriInfo) {
        final List<NameLinkRepresentation> jobs = new ArrayList<NameLinkRepresentation>();
        for (String name : jobService.listJobNames()) {
            final URI uri = uriInfo.getBaseUriBuilder().path(JobInfoResource.class).path(name).build();
            jobs.add(new NameLinkRepresentation(name, uri.getPath()));
        }
        return Response.ok(new JobsListRepresentation<NameLinkRepresentation>(jobs)).build();
    }

    @POST
    @Path("/{name}")
    //@Produces(MediaType.TEXT_PLAIN)
    public Response executeJob(@PathParam("name") final String name, @Context final UriInfo uriInfo)  {
        try {
            final String jobId = jobService.executeJob(name, true);
            final URI uri = uriInfo.getBaseUriBuilder().path(JobInfoResource.class).path(jobId).build();
            return Response.created(uri).build();
        } catch (JobException e) {
            return Response.status(Response.Status.NOT_FOUND).entity(e.getMessage()).build();
        }
    }

    @GET
    @Path("/history")
    public Response getJobs(@QueryParam("hours") @DefaultValue("12") final int hoursBack) {
        final Set<String> jobNames = jobService.listJobNames();
        final Map<String, List<JobInfoRepresentation>> jobs = new HashMap<String, List<JobInfoRepresentation>>();
        final Date dt = new Date(new Date().getTime() - 1000 * 60 * hoursBack);
        for (String jobName : jobNames) {
            final List<JobInfo> jobInfos = jobInfoRepository.findByNameAndTimeRange(jobName, dt, null);
            final List<JobInfoRepresentation> jobInfosRes = new ArrayList<JobInfoRepresentation>();
            for (JobInfo ji : jobInfos) {
                jobInfosRes.add(JobInfoRepresentation.fromJobInfo(ji));
            }
            jobs.put(jobName, jobInfosRes);
        }
        return Response.ok(jobs).build();
    }

    /*




    @GET
    @Path("/{name}/{id}")
    @Produces({MediaType.OTTO_JOB_XML, MediaType.OTTO_JOB_JSON})
    public Response getJob(@PathParam("name") final String name, @PathParam("id") final String id) {
        if (!ObjectId.isValid(id)) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("Could not get job, because invalid jobId=" + id + " specified").type(MediaType.TEXT_PLAIN).build();
        }
        //
        final JobInfo jobInfo = jobInfoRepository.findById(id);
        if (jobInfo == null || !jobInfo.getName().equalsIgnoreCase(name)) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(new JobInfoResource(jobInfo)).build();
    }

    private Response getLastJob(final String name) {
        //
        final JobInfo jobInfo = jobInfoRepository.findLastBy(name);

        if (jobInfo == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(new JobInfoResource(jobInfo)).build();
    }


    @GET
    @Produces({MediaType.OTTO_JOB_XML, MediaType.OTTO_JOB_JSON})
    public Response getJobById(@QueryParam("id") final String id, @Context HttpServletRequest httpServletRequest) throws URISyntaxException {
        if (!ObjectId.isValid(id)) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("Could not get job, because invalid jobId=" + id + " specified").type(MediaType.TEXT_PLAIN).build();
        }
        //
        final JobInfo jobInfo = jobInfoRepository.findById(id);
        if (jobInfo == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        // ~
        StringBuilder linkStrBld = new StringBuilder();
        linkStrBld.append(ControllerHelper.getContextPathUri(httpServletRequest));
        linkStrBld.append("/api/jobs/").append(jobInfo.getName()).append("/").append(jobInfo.getId());
        return Response.ok(new JobInfoResource(jobInfo)).contentLocation(new URI(linkStrBld.toString())).build();
    }


    @GET
    @Path("/{jobNameOrId}")
    @Produces({MediaType.OTTO_JOB_XML, MediaType.OTTO_JOB_JSON})
    public Response getJob(@PathParam("jobNameOrId") final String jobNameOrId) {
        if (!ObjectId.isValid(jobNameOrId)) {
            return getLastJob(jobNameOrId);
        }
        // ~
        final JobInfo jobInfo = jobInfoRepository.findById(jobNameOrId);
        if (jobInfo == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(new JobInfoResource(jobInfo)).build();
    }

    */

}
