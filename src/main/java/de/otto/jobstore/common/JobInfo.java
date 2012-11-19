package de.otto.jobstore.common;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.otto.jobstore.common.properties.JobInfoProperty;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public final class JobInfo extends AbstractItem {

    private static final long serialVersionUID = 2454224303569320787L;
    private static final DateFormat LOG_LINE_DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy-HH:mm:ss zzz", Locale.ENGLISH);

    public JobInfo(DBObject dbObject) {
        super(dbObject);
    }

    public JobInfo(String name, String host, String thread, long timeout) {
        this(name, host, thread, timeout, RunningState.RUNNING);
    }

    public JobInfo(String name, String host, String thread, long timeout, RunningState state) {
        this(name, host, thread, timeout, state, false, null);
    }

    public JobInfo(String name, String host, String thread, long maxExecutionTime, RunningState state, boolean forceExecution, Map<String, String> additionalData) {
        final Date dt = new Date();
        addProperty(JobInfoProperty.NAME, name);
        addProperty(JobInfoProperty.HOST, host);
        addProperty(JobInfoProperty.THREAD, thread);
        addProperty(JobInfoProperty.CREATION_TIME, dt);
        addProperty(JobInfoProperty.FORCE_EXECUTION, forceExecution);
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

    public Boolean isForceExecution() {
        return getProperty(JobInfoProperty.FORCE_EXECUTION);
    }

    public Date getStartTime() {
        return getProperty(JobInfoProperty.CREATION_TIME);
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
            return new HashMap<String, String>();
        } else {
            return additionalData.toMap();
        }
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
        for (final Iterator<LogLine> logLineIterator = getLogLines().iterator(); logLineIterator.hasNext(); ) {
            final LogLine logLine = logLineIterator.next();
            out.append(LOG_LINE_DATE_FORMAT.format(logLine.getTimestamp())).
                append(": ").append(logLine.getLine());
            if (logLineIterator.hasNext()) {
                out. append(System.getProperty("line.separator"));
            }
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
                "\", \"startTime\":\"" + getStartTime() +
                "\", \"timeout\":\"" + getMaxExecutionTime() +
                "\", \"finishTime\":\"" + getFinishTime() +
                "\", \"lastModifiedTime\":\"" + getLastModifiedTime() +
                "\", \"additionalData\":\"" + getAdditionalData().toString() +
                "\"}}";
    }

}
