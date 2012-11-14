package de.otto.jobstore.repository.impl;


import com.mongodb.Mongo;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

public class MongoIdRepositoryIntegrationTest {

    private MongoIdRepository idRepository;

    @BeforeClass
    public void init() throws Exception {
        Mongo mongo = new Mongo("127.0.0.1");
        idRepository = new MongoIdRepository(mongo, "lhotse-jobs", "ids_test");
    }

    @BeforeMethod
    public void setup() throws Exception {
        idRepository.clear(true);
    }

    @Test
    public void testGettingValueOfNotExistingId() throws Exception {
        Long id = idRepository.getId("test");
        assertThat(id, notNullValue());
        assertThat(id, is(0L));
    }

    @Test
    public void testGettingValueOfExistingId() throws Exception {
        idRepository.getId("test");
        Long id2 = idRepository.getId("test");
        assertThat(id2, is(1L));
    }
}
