package de.otto.jobstore.common;

import com.mongodb.DBObject;

import java.util.Date;

public final class LogLine extends AbstractItem<LogLine> {

    private static final String LINE = "line";
    private static final String TIMESTAMP = "timestamp";

    public LogLine() {
        super();
    }

    public LogLine(DBObject dbObject) {
        super(dbObject);
    }

    public LogLine(String line, Date timestamp) {
        addProperty(LINE, line);
        addProperty(TIMESTAMP, timestamp);
    }

    public String getLine() {
        return getProperty(LINE);
    }

    public Date getTimestamp() {
        return getProperty(TIMESTAMP);
    }


}
