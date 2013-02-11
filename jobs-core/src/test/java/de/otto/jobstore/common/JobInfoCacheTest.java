package de.otto.jobstore.common;


import com.mongodb.DBObject;
import de.otto.jobstore.repository.JobInfoRepository;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class JobInfoCacheTest {

    private JobInfoRepository jobInfoRepository = mock(JobInfoRepository.class);
    private final JobInfo jobInfo = new JobInfo(mock(DBObject.class));
    private final String id = "id";

    @Test
    public void testThatRepoIsHitOnlyOnce() throws Exception {
        reset(jobInfoRepository);
        when(jobInfoRepository.findById(id)).thenReturn(jobInfo);

        JobInfoCache jobInfoCache = new JobInfoCache(id, jobInfoRepository, 10000);
        Thread.sleep(100);
        jobInfoCache.isAborted();
        Thread.sleep(100);
        jobInfoCache.isAborted();

        verify(jobInfoRepository, times(1)).findById(id);
    }

    @Test
    public void testThatRepoIsHitTwice() throws Exception {
        reset(jobInfoRepository);
        when(jobInfoRepository.findById(id)).thenReturn(jobInfo);

        JobInfoCache jobInfoCache = new JobInfoCache(id, jobInfoRepository, 0);
        Thread.sleep(100);
        jobInfoCache.isAborted();

        verify(jobInfoRepository, times(2)).findById(id);
    }

}
