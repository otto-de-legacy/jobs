package de.otto.jobstore.web.representation;

import de.otto.jobstore.common.LogLine;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.StringWriter;
import java.util.Date;

import static org.testng.AssertJUnit.assertEquals;

public class LogLineRepresentationTest {

    @Test
    public void testFromLogLine() throws Exception {
        LogLine logLine = new LogLine("line", new Date());
        LogLineRepresentation llRep = LogLineRepresentation.fromLogLine(logLine);
        assertEquals(logLine.getLine(), llRep.getLine());
        assertEquals(logLine.getTimestamp(), llRep.getTimestamp());
    }

}
