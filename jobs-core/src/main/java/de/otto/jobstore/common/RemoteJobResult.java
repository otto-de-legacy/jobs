package de.otto.jobstore.common;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public final class RemoteJobResult {

    public boolean ok;

    @XmlElement(name = "exit_code")
    public int exitCode;

    public String message;

    public RemoteJobResult() {}

    public RemoteJobResult(boolean ok, int exitCode, String message) {
        this.ok = ok;
        this.exitCode = exitCode;
        this.message = message;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("RemoteJobResult");
        sb.append("{ok=").append(ok);
        sb.append(", exitCode=").append(exitCode);
        sb.append(", message='").append(message).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
