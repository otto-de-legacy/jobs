package de.otto.jobstore.common;

import de.otto.jobstore.repository.JobDefinitionRepository;

public class JobDefinitionQuery {

    private final String name;
    private final JobDefinitionRepository jobDefinitionRepository;

    public JobDefinitionQuery(String name, JobDefinitionRepository jobDefinitionRepository) {
        this.name = name;
        this.jobDefinitionRepository = jobDefinitionRepository;
    }

    public boolean isAborted() {
        return getJobDefinition().isAborted();
    }

    private StoredJobDefinition getJobDefinition() {
        return jobDefinitionRepository.find(name);
    }

}
