package de.otto.jobstore.web;

import java.util.List;
import java.util.Map;

interface ParameterExtractor {
    Map<String,List<String>> getQueryParameters();
}
