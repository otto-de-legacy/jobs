package de.otto.jobstore.service;


import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.ResultState;
import de.otto.jobstore.repository.JobInfoRepository;
import de.otto.jobstore.service.JobInfoService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

public class JobInfoServiceTest {

    private JobInfoService jobInfoService;
    private JobInfoRepository jobInfoRepository;

    @BeforeMethod
    public void setUp() throws Exception {
        jobInfoRepository = mock(JobInfoRepository.class);
        jobInfoService = new JobInfoService(jobInfoRepository);
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

    @Test
    public void testGetMostRecentExecutedList() throws Exception {
        when(jobInfoRepository.distinctJobNames()).thenReturn(Arrays.asList("test", "test2"));
        when(jobInfoRepository.findMostRecentByNameAndResultState("test",
                EnumSet.complementOf(EnumSet.of(ResultState.NOT_EXECUTED)))).thenReturn(new JobInfo("test", "host", "thread", 1234L));
        when(jobInfoRepository.findMostRecentByNameAndResultState("test2",
                EnumSet.complementOf(EnumSet.of(ResultState.NOT_EXECUTED)))).thenReturn(null);

        List<JobInfo> jobInfoList = jobInfoService.getMostRecentExecuted();
        assertEquals(1, jobInfoList.size());
        assertEquals("test", jobInfoList.get(0).getName());
    }
}
