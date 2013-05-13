package de.otto.jobstore.service;


import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.ResultCode;
import de.otto.jobstore.repository.JobInfoRepository;
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
        when(jobInfoRepository.findMostRecentFinished("test")).thenReturn(new JobInfo("test", "host", "thread", 1234L, 1234L, 0L));

        JobInfo jobInfo = jobInfoService.getMostRecentExecuted("test");
        assertEquals("test", jobInfo.getName());
        assertEquals(Long.valueOf(1234L), jobInfo.getMaxIdleTime());
    }

    @Test
    public void testGetMostRecentSuccessful() throws Exception {
        when(jobInfoRepository.findMostRecentByNameAndResultState("test",
               EnumSet.of(ResultCode.SUCCESSFUL))).thenReturn(new JobInfo("test", "host", "thread", 1234L, 1234L, 0L));

        JobInfo jobInfo = jobInfoService.getMostRecentSuccessful("test");
        assertEquals("test", jobInfo.getName());
        assertEquals(Long.valueOf(1234L), jobInfo.getMaxIdleTime());
    }

    @Test
    public void testGetMostRecentExecutedList() throws Exception {
        when(jobInfoRepository.distinctJobNames()).thenReturn(Arrays.asList("test", "test2"));
        when(jobInfoRepository.findMostRecentFinished("test")).thenReturn(new JobInfo("test", "host", "thread", 1234L, 1234L, 0L));
        when(jobInfoRepository.findMostRecentFinished("test2")).thenReturn(null);

        List<JobInfo> jobInfoList = jobInfoService.getMostRecentExecuted();
        assertEquals(1, jobInfoList.size());
        assertEquals("test", jobInfoList.get(0).getName());
    }
}
