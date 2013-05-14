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

    public JobInfo(String name, String host, String thread, Long maxIdleTime, Long maxExecutionTime) {
        this(name, host, thread, maxIdleTime, maxExecutionTime, RunningState.QUEUED);
    }

    public JobInfo(String name, String host, String thread, Long maxIdleTime, Long maxExecutionTime, RunningState state) {
        this(name, host, thread, maxIdleTime, maxExecutionTime, state, JobExecutionPriority.CHECK_PRECONDITIONS, null);
    }

    public JobInfo(Date dt, String name, String host, String thread, Long maxIdleTime, Long maxExecutionTime, RunningState state) {
        this(dt, name, host, thread, maxIdleTime, maxExecutionTime, state, JobExecutionPriority.CHECK_PRECONDITIONS, null);
    }

    public JobInfo(String name, String host, String thread, Long maxIdleTime, Long maxExecutionTime, RunningState state, JobExecutionPriority executionPriority, Map<String, String> additionalData) {
        this(new Date(), name, host, thread, maxIdleTime, maxExecutionTime, state, executionPriority, additionalData);
    }

    public JobInfo(Date dt, String name, String host, String thread, Long maxIdleTime, Long maxExecutionTime, RunningState state, JobExecutionPriority executionPriority, Map<String, String> additionalData) {
        addProperty(JobInfoProperty.NAME, name);
        addProperty(JobInfoProperty.HOST, host);
        addProperty(JobInfoProperty.THREAD, thread);
        if (state != RunningState.QUEUED) {
            addProperty(JobInfoProperty.START_TIME, dt);
        }
        addProperty(JobInfoProperty.CREATION_TIME, dt);
        addProperty(JobInfoProperty.EXECUTION_PRIORITY, executionPriority.name());
        addProperty(JobInfoProperty.RUNNING_STATE, state.name());
        setLastModifiedTime(dt);
        addProperty(JobInfoProperty.MAX_IDLE_TIME, maxIdleTime);
        addProperty(JobInfoProperty.MAX_EXECUTION_TIME, maxExecutionTime);

        if (additionalData != null) {
            addProperty(JobInfoProperty.ADDITIONAL_DATA, new BasicDBObject(additionalData));
        }
    }

    public boolean hasLowerPriority(JobExecutionPriority priority) {
        return getExecutionPriority().hasLowerPriority(priority);
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

    public Map<String, String> getParameters() {
        return getProperty(JobInfoProperty.PARAMETERS);
    }

    public void setParameters(Map<String, String> parameters) {
        addProperty(JobInfoProperty.PARAMETERS, parameters);
    }

    public Long getMaxIdleTime() {
        final Long maxIdleTime = getProperty(JobInfoProperty.MAX_IDLE_TIME);
        //TODO This is only for backward compability for the time, the new version of this library is running, with formerly started jobs
        if(maxIdleTime == null){
            return getProperty(JobInfoProperty.TIMEOUT_PERIOD);
        }
        return maxIdleTime;
    }

    public Long getMaxExecutionTime() {
        final Long maxExecutionTime = getProperty(JobInfoProperty.MAX_EXECUTION_TIME);
        //TODO This is only for backward compability for the time, the new version of this library is running, with formerly started jobs
        if(maxExecutionTime == null){
            return Long.valueOf(1000*60*60*2);
        }
        return maxExecutionTime;
    }

    public Long getRetries() {
        Long retries = getProperty(JobInfoProperty.RETRIES);

        if(retries == null) {
            return 0L;
        }
        return retries;
    }

    private Date getJobIdleExceededTime() {
        return new Date(getLastModifiedTime().getTime() + getMaxIdleTime());
    }

    private Date getJobExpireTime() {
        return new Date(getStartTime().getTime() + getMaxExecutionTime());
    }

    public boolean isTimedOut() {
        return isTimedOut(new Date());
    }

    public boolean isTimedOut(Date currentDate) {
        if(isStarted()){
            return getJobExpireTime().before(currentDate);
        } else {
            return false;
        }
    }

    public boolean isIdleTimeExceeded() {
        return isIdleTimeExceeded(new Date());
    }

    public boolean isIdleTimeExceeded(Date currentDate) {
        return getJobIdleExceededTime().before(currentDate);
    }

    public boolean isStarted() {
        return getStartTime() != null;
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

    public String getResultMessage() {
        return getProperty(JobInfoProperty.RESULT_MESSAGE);
    }

    public String getStatusMessage() {
        return getProperty(JobInfoProperty.STATUS_MESSAGE);
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

        final List<LogLine> result = new ArrayList<>(logLines.size());
        for (DBObject logLine : logLines) {
            result.add(new LogLine(logLine));
        }
        return result;
    }

    public List<LogLine> getLastLogLines(int maxLines) {
        final List<DBObject> logLines = getProperty(JobInfoProperty.LOG_LINES);
        if (logLines == null) return Collections.emptyList();

        final List<LogLine> result = new ArrayList<>(Math.min(logLines.size(), maxLines));
        int endPos = logLines.size();
        int startPos = Math.max(0, endPos - maxLines);
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

    public boolean isAborted() {
       final Boolean aborted = getProperty(JobInfoProperty.ABORTED);
       return aborted == null ? false : aborted;
    }

    public ResultCode getResultState() {
        final String resultState = getProperty(JobInfoProperty.RESULT_STATE);
        if (resultState == null) {
            return null;
        } else {
            return ResultCode.valueOf(resultState);
        }
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
                "\", \"maxIdleTime\":\"" + getMaxIdleTime() +
                "\", \"maxExecutionTime\":\"" + getMaxExecutionTime() +
                "\", \"finishTime\":\"" + getFinishTime() +
                "\", \"lastModifiedTime\":\"" + getLastModifiedTime() +
                "\", \"additionalData\":\"" + getAdditionalData().toString() +
                "\"}}";
    }

}
