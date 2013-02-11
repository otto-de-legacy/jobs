package de.otto.jobstore.service;

import de.otto.jobstore.common.JobDefinitionCache;
import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.JobRunnable;
import de.otto.jobstore.common.ResultCode;
import de.otto.jobstore.common.example.SimpleAbortableJob;
import de.otto.jobstore.common.example.SimpleJobRunnableExample;
import de.otto.jobstore.repository.JobDefinitionRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Resource;

import static org.testng.AssertJUnit.assertEquals;

@ContextConfiguration(locations = {"classpath:spring/jobs-context.xml"})
public class JobServiceJobAbortionIntegrationTest extends AbstractTestNGSpringContextTests {

    @Resource
    private JobService jobService;

    @Resource
    private JobInfoService jobInfoService;

    @Resource
    private JobDefinitionRepository jobDefinitionRepository;

    @BeforeMethod
    public void setUp() throws Exception {
        jobDefinitionRepository.clear(false);
        jobService.initialize();
        JobDefinitionCache.setUpdateInterval(0);
    }

    @Test
    public void abortedJobFinishesWithStatusAborted() throws Exception {
        JobRunnable abortableJob = new SimpleAbortableJob();
        jobService.registerJob(abortableJob);

        final String name = abortableJob.getJobDefinition().getName();
        String id = jobService.executeJob(name);
        jobService.setJobAbortionEnabled(name, true);
        Thread.sleep(500);

        JobInfo abortedJob = jobInfoService.getById(id);
        assertEquals(ResultCode.ABORTED, abortedJob.getResultState());
    }

    @Test
    public void nonAbortableJobIgnoresAbortingOfJob() throws Exception {
        JobRunnable nonAbortableJob = new SimpleJobRunnableExample();
        jobService.registerJob(nonAbortableJob);

        final String name = nonAbortableJob.getJobDefinition().getName();
        String id = jobService.executeJob(name);
        jobService.setJobAbortionEnabled(name, true);
        Thread.sleep(500);

        JobInfo finishedJob = jobInfoService.getById(id);
        assertEquals(ResultCode.SUCCESSFUL, finishedJob.getResultState());
    }
}
