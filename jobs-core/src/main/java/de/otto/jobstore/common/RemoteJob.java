package de.otto.jobstore.common;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.Serializable;
import java.util.Map;

public final class RemoteJob implements Serializable {

    public String name;
    public String client_id;
    public Map<String, String> parameters;

    public RemoteJob(String name, String client_id, Map<String, String> parameters) {
        this.name = name;
        this.client_id = client_id;
        this.parameters = parameters;
    }

    public JSONObject toJsonObject() throws JSONException {
        final JSONObject obj = new JSONObject();
        obj.put("name", name);
        obj.put("client_id", client_id);
        final JSONObject params = new JSONObject();
        for (String paramName : parameters.keySet()) {
            params.put(paramName, parameters.get(paramName));
        }
        obj.put("parameters", params);
        return obj;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("RemoteJob");
        sb.append("{name='").append(name).append('\'');
        sb.append(", client_id='").append(client_id).append('\'');
        sb.append(", parameters=").append(parameters);
        sb.append('}');
        return sb.toString();
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
