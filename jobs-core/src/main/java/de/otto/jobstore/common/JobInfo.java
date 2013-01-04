package de.otto.jobstore.common;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.otto.jobstore.common.properties.JobInfoProperty;

import java.util.*;


public final class JobInfo extends AbstractItem {

    private static final long serialVersionUID = 2454224303569320787L;

    private static final int MAX_LOGLINES = 100;

    public JobInfo(DBObject dbObject) {
        super(dbObject);
    }

    public JobInfo(String name, String host, String thread, Long maxExecutionTime) {
        this(name, host, thread, maxExecutionTime, RunningState.RUNNING);
    }

    public JobInfo(String name, String host, String thread, Long maxExecutionTime, RunningState state) {
        this(name, host, thread, maxExecutionTime, state, JobExecutionPriority.CHECK_PRECONDITIONS, null);
    }

    public JobInfo(String name, String host, String thread, Long maxExecutionTime, RunningState state, JobExecutionPriority executionPriority, Map<String, String> additionalData) {
        final Date dt = new Date();
        addProperty(JobInfoProperty.NAME, name);
        addProperty(JobInfoProperty.HOST, host);
        addProperty(JobInfoProperty.THREAD, thread);
        if(state != RunningState.QUEUED) {
            addProperty(JobInfoProperty.START_TIME, dt);
        }
        addProperty(JobInfoProperty.CREATION_TIME, dt);
        addProperty(JobInfoProperty.EXECUTION_PRIORITY, executionPriority.name());
        addProperty(JobInfoProperty.RUNNING_STATE, state.name());
        setLastModifiedTime(dt);
        addProperty(JobInfoProperty.MAX_EXECUTION_TIME, maxExecutionTime);
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

    public Long getMaxExecutionTime() {
        return getProperty(JobInfoProperty.MAX_EXECUTION_TIME);
    }

    public Date getJobExpireTime() {
        return new Date(getLastModifiedTime().getTime() + getMaxExecutionTime());
    }

    public JobExecutionPriority getExecutionPriority() {
        final String priority = getProperty(JobInfoProperty.EXECUTION_PRIORITY);
        if (priority == null) {
            return null;
        } else {
            return JobExecutionPriority.valueOf(priority);
        }
    }

    public Date getCreationTime() {
        return getProperty(JobInfoProperty.CREATION_TIME);
    }

    public Date getStartTime() {
        return getProperty(JobInfoProperty.START_TIME);
    }

    public Date getFinishTime() {
        return getProperty(JobInfoProperty.FINISH_TIME);
    }

    public String getErrorMessage() {
        return getProperty(JobInfoProperty.ERROR_MESSAGE);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getAdditionalData() {
        final DBObject additionalData = getProperty(JobInfoProperty.ADDITIONAL_DATA);
        if (additionalData == null) {
            return new HashMap<>();
        } else {
            return additionalData.toMap();
        }
    }

    public void putAdditionalData(String key, String value) {
        final DBObject additionalData;
        if (hasProperty(JobInfoProperty.ADDITIONAL_DATA)) {
            additionalData = getProperty(JobInfoProperty.ADDITIONAL_DATA);
        } else {
            additionalData = new BasicDBObject();
            addProperty(JobInfoProperty.ADDITIONAL_DATA, additionalData);
        }
        additionalData.put(key, value);
    }

    public void appendLogLine(LogLine logLine) {
        // TODO: should we also only store the most recent MAX_LOGLINES lines?
        if (logLine != null) {
            final List<DBObject> logLines;
            if (hasProperty(JobInfoProperty.LOG_LINES)) {
                logLines = getProperty(JobInfoProperty.LOG_LINES);
            } else {
                logLines = new ArrayList<>();
                addProperty(JobInfoProperty.LOG_LINES, logLines);
            }
            logLines.add(logLine.toDbObject());
        }
    }

    public boolean hasLogLines() {
        final List<DBObject> logLines = getProperty(JobInfoProperty.LOG_LINES);
        return logLines != null && !logLines.isEmpty();
    }

    public List<LogLine> getLogLines() {
        final List<DBObject> logLines = getProperty(JobInfoProperty.LOG_LINES);
        if (logLines == null) return Collections.emptyList();

        final List<LogLine> result = new ArrayList<>(Math.min(logLines.size(), MAX_LOGLINES));
        int endPos = logLines.size();
        int startPos = Math.max(0, endPos - MAX_LOGLINES);
        for (DBObject logLine : logLines.subList(startPos, endPos)) {
            result.add(new LogLine(logLine));
        }
        return result;
    }

    public Date getLastModifiedTime() {
        return getProperty(JobInfoProperty.LAST_MODIFICATION_TIME);
    }

    public void setLastModifiedTime(Date lastModifiedTime) {
        addProperty(JobInfoProperty.LAST_MODIFICATION_TIME, lastModifiedTime);
    }

    public String getRunningState() {
        return getProperty(JobInfoProperty.RUNNING_STATE);
    }

    public ResultState getResultState() {
        final String resultState = getProperty(JobInfoProperty.RESULT_STATE);
        if (resultState == null) {
            return null;
        } else {
            return ResultState.valueOf(resultState);
        }
    }

    public boolean isTimedOut() {
        return isTimedOut(new Date());
    }

    public boolean isTimedOut(Date currentDate) {
        return getJobExpireTime().before(currentDate);
    }

    @Override
    public String toString() {
        return "{\"JobInfo\" : {" +
                "\"id\":\"" + getId() +
                "\", \"name\":\"" + getName() +
                "\", \"host\":\"" + getHost() +
                "\", \"thread\":\"" + getThread() +
                "\", \"creationTime\":\"" + getCreationTime() +
                "\", \"startTime\":\"" + getStartTime() +
                "\", \"timeout\":\"" + getMaxExecutionTime() +
                "\", \"finishTime\":\"" + getFinishTime() +
                "\", \"lastModifiedTime\":\"" + getLastModifiedTime() +
                "\", \"additionalData\":\"" + getAdditionalData().toString() +
                "\"}}";
    }

}
