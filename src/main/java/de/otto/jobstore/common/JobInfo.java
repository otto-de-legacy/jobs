package de.otto.jobstore.common;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public final class JobInfo extends AbstractItem<JobInfo> {

    private static final long serialVersionUID = 2454224303569320787L;

    public static final String NAME = "name";
    public static final String HOST = "host";
    public static final String THREAD = "thread";
    public static final String START_TIME = "startTime";
    public static final String FINISH_TIME = "finishTime";
    public static final String ERROR_MESSAGE = "errorMessage";
    public static final String RUNNING_STATE = "runningState";
    public static final String RESULT_STATE = "resultState";
    private static final String TIMEOUT = "timeout";
    public static final String LAST_MODIFICATION_TIME = "lastModificationTime";
    public static final String ADDITIONAL_DATA = "additionalData";
    public static final String LOG_LINES = "logLines";

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
        addProperty(NAME, name);
        addProperty(HOST, host);
        addProperty(THREAD, thread);
        addProperty(START_TIME, dt);
        setRunningState(state.name());
        setLastModifiedTime(dt);
        addProperty(TIMEOUT, timeout);
        setAdditionalData(additionalData);
    }

    public String getId() {
        final Object id = getProperty("_id");
        if (id == null) {
            return null;
        } else {
            return id.toString();
        }
    }

    public String getName() {
        return getProperty(NAME);
    }

    public String getHost() {
        return getProperty(HOST);
    }

    public String getThread() {
        return getProperty(THREAD);
    }

    public Long getTimeout() {
        return getProperty(TIMEOUT);
    }

    public Date getJobExpireTime() {
        return new Date(getLastModifiedTime().getTime() + getTimeout());
    }

    public Date getStartTime() {
        return getProperty(START_TIME);
    }

    public Date getFinishTime() {
        return getProperty(FINISH_TIME);
    }

    public void setFinishTime(Date finishTime) {
        addProperty(FINISH_TIME, finishTime);
    }

    public String getErrorMessage() {
        return getProperty(ERROR_MESSAGE);
    }

    public void setErrorMessage(String errorMessage) {
        addProperty(ERROR_MESSAGE, errorMessage);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getAdditionalData() {
        final DBObject additionalData = getProperty(ADDITIONAL_DATA);
        if (additionalData == null) {
            return null;
        } else {
            return additionalData.toMap();
        }
    }

    public void setAdditionalData(Map<String, String> additionalData) {
        if (additionalData != null) {
            addProperty(ADDITIONAL_DATA, new BasicDBObject(additionalData));
        }
    }

    public void appendLogLine(LogLine logLine) {
        final List<DBObject> logLines;
        if (hasProperty(LOG_LINES)) {
            logLines = getProperty(LOG_LINES);
        } else {
            logLines = new ArrayList<DBObject>();
            addProperty(LOG_LINES, logLines);
        }
        logLines.add(logLine.toDbObject());
    }

    public List<LogLine> getLogLines() {
        final List<DBObject> logLines = getProperty(LOG_LINES);
        final List<LogLine> result = new ArrayList<LogLine>();
        for (DBObject logLine : logLines) {
            result.add(new LogLine(logLine));
        }
        return result;
    }

    public String getLogLineString() {
        if (getLogLines() == null) {
            return null;
        }
        //
        final StringBuilder out = new StringBuilder();
        for (LogLine logLine : getLogLines()) {
            //TODO: out.append(new DateTime(logLine.getTimestamp()));
            out.append(logLine.getTimestamp()).
                append(": ").
                append(logLine.getLine()).
                append(System.getProperty("line.separator"));
        }
        //TODO: return StringEscapeUtils.escapeHtml(out.toString());
        return out.toString();
    }

    public Date getLastModifiedTime() {
        return getProperty(LAST_MODIFICATION_TIME);
    }

    public void setLastModifiedTime(Date lastModifiedTime) {
        addProperty(LAST_MODIFICATION_TIME, lastModifiedTime);
    }

    public String getRunningState() {
        return getProperty(RUNNING_STATE);
    }

    public void setRunningState(String runningState) {
        addProperty(RUNNING_STATE, runningState);
    }

    public ResultState getResultState() {
        final String resultState = getProperty(RESULT_STATE);
        if (resultState == null) {
            return null;
        } else {
            return ResultState.valueOf(resultState);
        }
    }

    public void setResultState(ResultState resultState) {
        addProperty(RESULT_STATE, resultState.name());
    }

    public boolean isTimeoutReached() {
        long currentTicks = new Date().getTime();
        return currentTicks > getJobExpireTime().getTime();
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
                //TODO: "\", \"additionalData\":\"" + StringUtils.unicodeEscape(additionalData.toString()) +
                "\", \"additionalData\":\"" + getAdditionalData().toString() +
                "\"}}";
    }

}
