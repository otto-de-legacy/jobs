package de.otto.jobstore.repository;

import de.otto.jobstore.common.StoredJobDefinition;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Resource;

import static org.testng.AssertJUnit.*;

@ContextConfiguration(locations = {"classpath:spring/lhotse-jobs-context.xml"})
public class JobDefinitionRepositoryIntegrationTest extends AbstractTestNGSpringContextTests {

    private static final String JOB_NAME = "test";

    @Resource
    private JobDefinitionRepository jobDefinitionRepository;

    @BeforeMethod
    public void setUp() throws Exception {
        jobDefinitionRepository.clear(true);
    }

    @Test
    public void testAddingNotExistingJobDefinition() throws Exception {
        StoredJobDefinition jd = new StoredJobDefinition(JOB_NAME, 1, 1, true);
        jobDefinitionRepository.addOrUpdate(jd);
        StoredJobDefinition retrievedJobDefinition = jobDefinitionRepository.find(JOB_NAME);
        assertNotNull(retrievedJobDefinition);
    }

    @Test
    public void testUpdatingExistingJobDefinition() throws Exception {
        StoredJobDefinition jd = new StoredJobDefinition(JOB_NAME, 1, 1, true);
        jobDefinitionRepository.addOrUpdate(jd);
        StoredJobDefinition retrievedJobDefinition = jobDefinitionRepository.find(JOB_NAME);
        assertEquals(1L, retrievedJobDefinition.getPollingInterval());

        jd = new StoredJobDefinition(JOB_NAME, 1, 2, true);
        jobDefinitionRepository.addOrUpdate(jd);
        retrievedJobDefinition = jobDefinitionRepository.find(JOB_NAME);
        assertEquals(2L, retrievedJobDefinition.getPollingInterval());
    }

    @Test
    public void testPausingJob() throws Exception {
        StoredJobDefinition jd = new StoredJobDefinition(JOB_NAME, 1, 1, true);
        jobDefinitionRepository.save(jd);
        jobDefinitionRepository.enableJob(JOB_NAME);

        StoredJobDefinition retrievedJobDefinition = jobDefinitionRepository.find(JOB_NAME);
        assertTrue(retrievedJobDefinition.isDisabled());
    }

    @Test
    public void testActivatingJob() throws Exception {
        StoredJobDefinition jd = new StoredJobDefinition(JOB_NAME, 1, 1, true);
        jd.setDisabled(true);
        jobDefinitionRepository.save(jd);
        jobDefinitionRepository.disableJob(JOB_NAME);

        StoredJobDefinition retrievedJobDefinition = jobDefinitionRepository.find(JOB_NAME);
        assertFalse(retrievedJobDefinition.isDisabled());
    }

    @Test
    public void testUpdatingPausedJob() throws Exception {
        StoredJobDefinition jd = new StoredJobDefinition(JOB_NAME, 1, 1, true);
        jd.setDisabled(true);
        jobDefinitionRepository.save(jd);

        StoredJobDefinition jd2 = new StoredJobDefinition(JOB_NAME, 2, 2, true);
        jobDefinitionRepository.addOrUpdate(jd2);
        StoredJobDefinition retrievedJobDefinition = jobDefinitionRepository.find(JOB_NAME);
        assertEquals(2, retrievedJobDefinition.getPollingInterval());
        assertTrue(retrievedJobDefinition.isDisabled());
    }
}
