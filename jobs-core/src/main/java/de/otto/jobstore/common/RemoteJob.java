package de.otto.jobstore.common;


import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;

public final class RemoteJob {

    public String name;
    public List<Parameter> parameters;

    public RemoteJob() {}

    public RemoteJob(String name, List<Parameter> parameters) {
        this.name = name;
        this.parameters = parameters;
    }

    public JSONObject toJsonObject() throws JSONException {
        final JSONObject obj = new JSONObject();
        obj.put("name", name);
        final JSONObject params = new JSONObject();
        for (Parameter parameter : parameters) {
            params.put(parameter.key, parameter.value);
        }
        obj.put("parameters", params);
        return obj;
    }

}
