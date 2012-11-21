package de.otto.jobstore.web.representation;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "job")
@XmlAccessorType(value = XmlAccessType.FIELD)
public final class JobNameRepresentation {

    private String name;

    public JobNameRepresentation() {}

    public JobNameRepresentation(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
