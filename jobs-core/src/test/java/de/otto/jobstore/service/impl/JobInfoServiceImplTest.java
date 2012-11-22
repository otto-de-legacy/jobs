package de.otto.jobstore.service.impl;


import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.ResultState;
import de.otto.jobstore.repository.api.JobInfoRepository;
import de.otto.jobstore.service.api.JobInfoService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.EnumSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

public class JobInfoServiceImplTest {

    private JobInfoService jobInfoService;
    private JobInfoRepository jobInfoRepository;

    @BeforeMethod
    public void setUp() throws Exception {
        jobInfoRepository = mock(JobInfoRepository.class);
        jobInfoService = new JobInfoServiceImpl(jobInfoRepository);
    }

    @Test
    public void testGetMostRecentExecuted() throws Exception {
        when(jobInfoRepository.findMostRecentByNameAndResultState("test",
                EnumSet.complementOf(EnumSet.of(ResultState.NOT_EXECUTED)))).thenReturn(new JobInfo("test", "host", "thread", 1234L));

        JobInfo jobInfo = jobInfoService.getMostRecentExecuted("test");
        assertEquals("test", jobInfo.getName());
        assertEquals(Long.valueOf(1234L), jobInfo.getMaxExecutionTime());
    }

    @Test
    public void testGetMostRecentSuccessful() throws Exception {
        when(jobInfoRepository.findMostRecentByNameAndResultState("test",
               EnumSet.of(ResultState.SUCCESSFUL))).thenReturn(new JobInfo("test", "host", "thread", 1234L));

        JobInfo jobInfo = jobInfoService.getMostRecentSuccessful("test");
        assertEquals("test", jobInfo.getName());
        assertEquals(Long.valueOf(1234L), jobInfo.getMaxExecutionTime());
    }


}
