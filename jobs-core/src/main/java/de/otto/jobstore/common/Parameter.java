package de.otto.jobstore.common;

import java.io.Serializable;

public final class Parameter implements Serializable {

    public final String key;
    public final String value;

    public Parameter(String key, String value) {
        this.key = key;
        this.value = value;
    }

}
