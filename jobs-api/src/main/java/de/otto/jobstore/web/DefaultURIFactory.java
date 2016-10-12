package de.otto.jobstore.web;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;

class DefaultURIFactory implements URIFactory {
    private final Class<?> aClass;
    private final UriInfo uriInfo;

    DefaultURIFactory(Class<?> aClass, UriInfo uriInfo) {
        this.aClass = aClass;
        this.uriInfo = uriInfo;
    }

    private UriBuilder getUriBase() {
        return uriInfo.getBaseUriBuilder().path(aClass);
    }

    @Override
    public URI create() {
        return getUriBase().build();
    }

    @Override
    public URI create(String name) {
        return getUriBase().path(name).build();
    }

    @Override
    public URI create(String name, String id) {
        return getUriBase().path(name).path(id).build();
    }
}
