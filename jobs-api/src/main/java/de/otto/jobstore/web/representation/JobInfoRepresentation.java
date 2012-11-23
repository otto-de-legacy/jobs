package de.otto.jobstore.web.representation;

import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.LogLine;
import de.otto.jobstore.common.ResultState;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@XmlRootElement(name = "jobInfo")
@XmlAccessorType(value = XmlAccessType.FIELD)
public final class JobInfoRepresentation {

    private String id;

    private String name;

    private String host;

    private String thread;

    private Date creationTime;

    private Date finishTime;

    private String errorMessage;

    private String runningState;

    private ResultState resultState;

    private Long maxExecutionTime;

    private Date lastModifiedTime;

    private Map<String, String> additionalData;

    private LogLinesRepresentation logLines = new LogLinesRepresentation();

    public JobInfoRepresentation() {}

    private JobInfoRepresentation(String id, String name, String host, String thread, Date creationTime, Date finishTime,
                                  String errorMessage, String runningState, ResultState resultState, Long maxExecutionTime,
                                  Date lastModifiedTime, Map<String, String> additionalData, LogLinesRepresentation logLines) {
        this.id = id;
        this.name = name;
        this.host = host;
        this.thread = thread;
        this.creationTime = creationTime;
        this.finishTime = finishTime;
        this.errorMessage = errorMessage;
        this.runningState = runningState;
        this.resultState = resultState;
        this.maxExecutionTime = maxExecutionTime;
        this.lastModifiedTime = lastModifiedTime;
        this.additionalData = additionalData;
        this.logLines = logLines;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public String getThread() {
        return thread;
    }

    public Date getCreationTime() {
        return creationTime;
    }

    public Date getFinishTime() {
        return finishTime;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getRunningState() {
        return runningState;
    }

    public ResultState getResultState() {
        return resultState;
    }

    public Long getMaxExecutionTime() {
        return maxExecutionTime;
    }

    public Date getLastModifiedTime() {
        return lastModifiedTime;
    }

    public Map<String, String> getAdditionalData() {
        return additionalData;
    }

    public LogLinesRepresentation getLogLines() {
        return logLines;
    }

    public static JobInfoRepresentation fromJobInfo(JobInfo jobInfo) {
        final List<LogLineRepresentation> logLines = new ArrayList<LogLineRepresentation>();
        for (LogLine ll : jobInfo.getLogLines()) {
            logLines.add(LogLineRepresentation.fromLogLine(ll));
        }
        return new JobInfoRepresentation(jobInfo.getId(), jobInfo.getName(), jobInfo.getHost(),
                jobInfo.getThread(), jobInfo.getCreationTime(), jobInfo.getFinishTime(),
                jobInfo.getErrorMessage(), jobInfo.getRunningState(), jobInfo.getResultState(),
                jobInfo.getMaxExecutionTime(), jobInfo.getLastModifiedTime(), jobInfo.getAdditionalData(),
                new LogLinesRepresentation(logLines));
    }

}
