package de.otto.jobstore.common;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public final class RemoteJobResult {

    public boolean ok;
    public int exitCode;
    public String message;

}
