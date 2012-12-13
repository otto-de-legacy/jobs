package de.otto.jobstore.common;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public final class Parameter {

    public String key;
    public String value;

    public Parameter() {}

    public Parameter(String key, String value) {
        this.key = key;
        this.value = value;
    }

}
