package de.otto.jobstore.common;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.otto.jobstore.common.properties.JobInfoProperty;

import java.util.*;


public final class JobInfo extends AbstractItem {

    private static final long serialVersionUID = 2454224303569320787L;

    public JobInfo(DBObject dbObject) {
        super(dbObject);
    }

    public JobInfo(String name, String host, String thread, long timeout) {
        this(name, host, thread, timeout, RunningState.RUNNING);
    }

    public JobInfo(String name, String host, String thread, long timeout, RunningState state) {
        this(name, host, thread, timeout, state, null);
    }

    public JobInfo(String name, String host, String thread, long timeout, RunningState state, Map<String, String> additionalData) {
        final Date dt = new Date();
        addProperty(JobInfoProperty.NAME, name);
        addProperty(JobInfoProperty.HOST, host);
        addProperty(JobInfoProperty.THREAD, thread);
        addProperty(JobInfoProperty.START_TIME, dt);
        setRunningState(state);
        setLastModifiedTime(dt);
        addProperty(JobInfoProperty.TIMEOUT, timeout);
        if (additionalData != null) {
            addProperty(JobInfoProperty.ADDITIONAL_DATA, new BasicDBObject(additionalData));
        }
    }

    public String getId() {
        final Object id = getProperty(JobInfoProperty.ID);
        if (id == null) {
            return null;
        } else {
            return id.toString();
        }
    }

    public String getName() {
        return getProperty(JobInfoProperty.NAME);
    }

    public String getHost() {
        return getProperty(JobInfoProperty.HOST);
    }

    public String getThread() {
        return getProperty(JobInfoProperty.THREAD);
    }

    public Long getTimeout() {
        return getProperty(JobInfoProperty.TIMEOUT);
    }

    public Date getJobExpireTime() {
        return new Date(getLastModifiedTime().getTime() + getTimeout());
    }

    public Date getStartTime() {
        return getProperty(JobInfoProperty.START_TIME);
    }

    public Date getFinishTime() {
        return getProperty(JobInfoProperty.FINISH_TIME);
    }

    public void setFinishTime(Date finishTime) {
        addProperty(JobInfoProperty.FINISH_TIME, finishTime);
    }

    public String getErrorMessage() {
        return getProperty(JobInfoProperty.ERROR_MESSAGE);
    }

    public void setErrorMessage(String errorMessage) {
        addProperty(JobInfoProperty.ERROR_MESSAGE, errorMessage);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getAdditionalData() {
        final DBObject additionalData = getProperty(JobInfoProperty.ADDITIONAL_DATA);
        if (additionalData == null) {
            return new HashMap<String, String>();
        } else {
            return additionalData.toMap();
        }
    }

    public void putAdditionalData(String key, String value) {
        getAdditionalData().put(key, value);
    }

    public void appendLogLine(LogLine logLine) {
        final List<DBObject> logLines;
        if (hasProperty(JobInfoProperty.LOG_LINES)) {
            logLines = getProperty(JobInfoProperty.LOG_LINES);
        } else {
            logLines = new ArrayList<DBObject>();
            addProperty(JobInfoProperty.LOG_LINES, logLines);
        }
        logLines.add(logLine.toDbObject());
    }

    public List<LogLine> getLogLines() {
        final List<DBObject> logLines = getProperty(JobInfoProperty.LOG_LINES);
        final List<LogLine> result = new ArrayList<LogLine>();
        if (logLines != null) {
            for (DBObject logLine : logLines) {
                result.add(new LogLine(logLine));
            }
        }
        return result;
    }

    public String getLogLineString() {
        if (getLogLines().isEmpty()) {
            return null;
        }
        //
        final StringBuilder out = new StringBuilder();
        for (LogLine logLine : getLogLines()) {
            out.append(logLine.getTimestamp()).
                append(": ").
                append(logLine.getLine()).
                append(System.getProperty("line.separator"));
        }
        return out.toString();
    }

    public Date getLastModifiedTime() {
        return getProperty(JobInfoProperty.LAST_MODIFICATION_TIME);
    }

    public void setLastModifiedTime(Date lastModifiedTime) {
        addProperty(JobInfoProperty.LAST_MODIFICATION_TIME, lastModifiedTime);
    }

    public RunningState getRunningState() {
        final String runningState = getProperty(JobInfoProperty.RUNNING_STATE);
        if (runningState == null) {
            return null;
        } else {
            return RunningState.valueOf(runningState);
        }
    }

    public void setRunningState(RunningState runningState) {
        addProperty(JobInfoProperty.RUNNING_STATE, runningState.name());
    }

    public ResultState getResultState() {
        final String resultState = getProperty(JobInfoProperty.RESULT_STATE);
        if (resultState == null) {
            return null;
        } else {
            return ResultState.valueOf(resultState);
        }
    }

    public void setResultState(ResultState resultState) {
        addProperty(JobInfoProperty.RESULT_STATE, resultState.name());
    }

    public boolean isTimeoutReached() {
        return new Date().getTime() > getJobExpireTime().getTime();
    }

    public boolean isExpired(Date currentDate) {
        return getJobExpireTime().before(currentDate);
    }

    @Override
    public String toString() {
        return "{\"JobInfo\" : {" +
                "\"id\":\"" + getId() +
                "\", \"name\":\"" + getName() +
                "\", \"host\":\"" + getHost() +
                "\", \"thread\":\"" + getThread() +
                "\", \"startTime\":\"" + getStartTime() +
                "\", \"timeout\":\"" + getTimeout() +
                "\", \"finishTime\":\"" + getFinishTime() +
                "\", \"lastModifiedTime\":\"" + getLastModifiedTime() +
                "\", \"additionalData\":\"" + getAdditionalData().toString() +
                "\"}}";
    }

}
