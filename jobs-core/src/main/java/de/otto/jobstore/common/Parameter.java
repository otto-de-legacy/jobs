package de.otto.jobstore.common;

import java.io.Serializable;

public final class Parameter implements Serializable {

    public String key;
    public String value;

    // TODO: to allow BSONEncoder of Mongo to serialize object
    public Parameter() {
    }

    public Parameter(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
