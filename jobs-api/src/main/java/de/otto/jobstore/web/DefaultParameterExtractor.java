package de.otto.jobstore.web;

import javax.ws.rs.core.UriInfo;
import java.util.List;
import java.util.Map;

class DefaultParameterExtractor implements ParameterExtractor {
    private final UriInfo uriInfo;

    DefaultParameterExtractor(UriInfo uriInfo) {
        this.uriInfo = uriInfo;
    }

    @Override
    public Map<String, List<String>> getQueryParameters() {
        return uriInfo.getQueryParameters();
    }
}
