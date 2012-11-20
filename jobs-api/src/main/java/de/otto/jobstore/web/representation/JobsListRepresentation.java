package de.otto.jobstore.web.representation;


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
@XmlAccessorType(value = XmlAccessType.FIELD)
public class JobsListRepresentation<E> {

    private final List<E> jobs;

    public JobsListRepresentation(List<E> jobs) {
        this.jobs = jobs;
    }

    public List<E> getJobs() {
        return jobs;
    }

}
