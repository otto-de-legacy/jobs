package de.otto.jobstore.web.representation;

import de.otto.jobstore.common.LogLine;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

@XmlRootElement
@XmlAccessorType(value = XmlAccessType.FIELD)
public final class LogLineRepresentation {

    private final String line;
    private final Date timestamp;

    private LogLineRepresentation(String line, Date timestamp) {
        this.line = line;
        this.timestamp = timestamp;
    }

    public String getLine() {
        return line;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public static LogLineRepresentation fromLogLine(LogLine ll) {
        return new LogLineRepresentation(ll.getLine(), ll.getTimestamp());
    }

}
