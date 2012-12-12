package de.otto.jobstore.common;


import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
public final class RemoteJob {

    private String name;
    private List<Parameter> parameters;

    public RemoteJob() {}

    public RemoteJob(String name, List<Parameter> parameters) {
        this.name = name;
        this.parameters = parameters;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

}
