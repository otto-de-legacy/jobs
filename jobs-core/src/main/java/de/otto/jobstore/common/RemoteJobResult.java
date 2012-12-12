package de.otto.jobstore.common;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public final class RemoteJobResult {

    private boolean ok;
    private int exitCode;
    private String message;

    public RemoteJobResult() {}

    public RemoteJobResult(boolean ok, int exitCode, String message) {
        this.ok = ok;
        this.exitCode = exitCode;
        this.message = message;
    }

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }

    public int getExitCode() {
        return exitCode;
    }

    public void setExitCode(int exitCode) {
        this.exitCode = exitCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
