package de.otto.jobstore.web;

import java.net.URI;

interface URIFactory {
    URI create();
    URI create(String name);
    URI create(String name, String id);
}
