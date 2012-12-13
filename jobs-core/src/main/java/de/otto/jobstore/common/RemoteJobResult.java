package de.otto.jobstore.common;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public final class RemoteJobResult {

    public boolean ok;
    public int exitCode;
    public String message;

    public RemoteJobResult() {}

    public RemoteJobResult(boolean ok, int exitCode, String message) {
        this.ok = ok;
        this.exitCode = exitCode;
        this.message = message;
    }

}
