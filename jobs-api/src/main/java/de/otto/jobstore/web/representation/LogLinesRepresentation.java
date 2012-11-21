package de.otto.jobstore.web.representation;


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Iterator;
import java.util.List;

@XmlRootElement
@XmlAccessorType(value = XmlAccessType.FIELD)
public final class LogLinesRepresentation implements Iterable<LogLineRepresentation> {

    @XmlElement(name = "logLine")
    private List<LogLineRepresentation> logLines;

    public LogLinesRepresentation() {}

    public LogLinesRepresentation(List<LogLineRepresentation> logLines) {
        this.logLines = logLines;
    }

    public List<LogLineRepresentation> getLogLines() {
        return logLines;
    }

    public int size() {
        return logLines.size();
    }

    @Override
    public Iterator<LogLineRepresentation> iterator() {
        return logLines.iterator();
    }
}
