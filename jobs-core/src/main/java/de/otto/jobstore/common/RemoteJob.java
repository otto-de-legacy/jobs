package de.otto.jobstore.common;


import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;

public final class RemoteJob {

    public String name;
    public List<Parameter> parameters;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteJob remoteJob = (RemoteJob) o;

        if (name != null ? !name.equals(remoteJob.name) : remoteJob.name != null) return false;
        if (parameters != null ? !parameters.equals(remoteJob.parameters) : remoteJob.parameters != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (parameters != null ? parameters.hashCode() : 0);
        return result;
    }

}
