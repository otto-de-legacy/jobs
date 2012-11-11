package de.otto.jobstore.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class InternetUtils {

    private InternetUtils() {}

    public static String getHostName() {
        try {
            final InetAddress address = InetAddress.getLocalHost();
            return address.getCanonicalHostName();
        } catch (UnknownHostException e) {
            return "N/A";
        }
    }

}
