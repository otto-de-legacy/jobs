package de.otto.jobstore.common;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
public final class RemoteJobStatus {

    public static enum Status {
        RUNNING,
        FINISHED
    }

    public Status status;

    @XmlElement(name = "log_lines")
    public List<String> logLines = new ArrayList<>();

    public RemoteJobResult result;

    @XmlElement(name = "finish_time")
    public String finishTime;

    public String message;

    public RemoteJobStatus() {
    }

    public RemoteJobStatus(Status status, List<String> logLines, RemoteJobResult result, String finishTime) {
        this.status = status;
        if  (logLines != null) {
            this.logLines = logLines;
        }
        this.result = result;
        this.finishTime = finishTime;
    }

    /**
     * Convenience constructor in case you are not finished yet,
     */
    public RemoteJobStatus(Status status, List<String> logLines, String message) {
        this.status = status;
        if  (logLines != null) {
            this.logLines = logLines;
        }
        this.message = message;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("RemoteJobStatus");
        sb.append("{status=").append(status);
        sb.append(", logLines=").append(logLines);
        sb.append(", result=").append(result);
        sb.append(", finishTime='").append(finishTime).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
