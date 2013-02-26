package de.otto.jobstore.web;


import de.otto.jobstore.common.*;
import de.otto.jobstore.repository.JobDefinitionRepository;
import de.otto.jobstore.service.JobInfoService;
import de.otto.jobstore.service.JobService;
import de.otto.jobstore.service.exception.JobException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Resource;
import javax.ws.rs.core.Response;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

@ContextConfiguration(locations = {"classpath:spring/api-context.xml"})
public class JobInfoResourceIntegrationTest extends AbstractTestNGSpringContextTests {

    @Resource
    private JobInfoResource jobInfoResource;

    @Resource
    private JobService jobService;

    @Resource
    private JobInfoService jobInfoService;

    @BeforeMethod
    public void setUp() throws Exception {
        jobService.clean();
    }

    @Test
    public void testThatAbortedJobHasAbortFlag() throws Exception {
        JobRunnable jobRunnable = mockRunnable(true);
        jobService.registerJob(jobRunnable);
        final String id = jobService.executeJob(jobRunnable.getJobDefinition().getName());

        final Response response = jobInfoResource.abortJob(jobRunnable.getJobDefinition().getName(), id);
        assertEquals(200, response.getStatus());
        final JobInfo jobInfo = jobInfoService.getById(id);
        assertTrue(jobInfo.isAborted());
    }

    @Test
    public void testAbortingNotAbortableJobResultsInError() throws Exception {
        JobRunnable jobRunnable = mockRunnable(false);
        jobService.registerJob(jobRunnable);
        final String id = jobService.executeJob(jobRunnable.getJobDefinition().getName());

        final Response response = jobInfoResource.abortJob(jobRunnable.getJobDefinition().getName(), id);
        assertEquals(403, response.getStatus());
    }

    private AbstractLocalJobRunnable mockRunnable(final boolean abortable) {
        return new AbstractLocalJobRunnable() {
            @Override
            public JobDefinition getJobDefinition() {
                return new AbstractLocalJobDefinition() {
                    @Override
                    public String getName() {
                        return "hans";
                    }

                    @Override
                    public long getMaxExecutionTime() {
                        return 0;
                    }

                    @Override
                    public long getMaxIdleTime() {
                        return 0;
                    }

                    @Override
                    public boolean isAbortable() {
                        return abortable;
                    }
                };
            }

            @Override
            public void execute(JobExecutionContext context) throws JobException {}
        };
    }
}
