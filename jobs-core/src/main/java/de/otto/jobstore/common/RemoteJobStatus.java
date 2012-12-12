package de.otto.jobstore.common;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
public final class RemoteJobStatus {

    public static enum Status {
        RUNNING,
        FINISHED
    }

    private Status status;
    private List<String> logLines;
    private RemoteJobResult result;

    public RemoteJobStatus() {}

    public RemoteJobStatus(Status status, List<String> logLines, RemoteJobResult result) {
        this.status = status;
        this.logLines = logLines;
        this.result = result;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<String> getLogLines() {
        return logLines;
    }

    public void setLogLines(List<String> logLines) {
        this.logLines = logLines;
    }

    public RemoteJobResult getResult() {
        return result;
    }

    public void setResult(RemoteJobResult result) {
        this.result = result;
    }
}
