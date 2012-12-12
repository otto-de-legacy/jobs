package de.otto.jobstore.common;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public final class Parameter {

    private String key;
    private String value;

    public Parameter() {}

    public Parameter(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

}
