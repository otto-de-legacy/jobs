package de.otto.jobstore.common;

import com.mongodb.DBObject;
import de.otto.jobstore.common.properties.LogLineProperty;

import java.util.Date;

public final class LogLine extends AbstractItem {

    public LogLine(DBObject dbObject) {
        super(dbObject);
    }

    public LogLine(String line, Date timestamp) {
        addProperty(LogLineProperty.LINE, line);
        addProperty(LogLineProperty.TIMESTAMP, timestamp);
    }

    public String getLine() {
        return getProperty(LogLineProperty.LINE);
    }

    public Date getTimestamp() {
        return getProperty(LogLineProperty.TIMESTAMP);
    }

}
