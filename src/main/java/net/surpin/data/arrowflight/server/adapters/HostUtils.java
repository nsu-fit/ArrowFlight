package net.surpin.data.arrowflight.server.adapters;

import java.net.InetAddress;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Normalizes hostnames and URIs to IP addresses with caching.
 * Accepts server URIs, domain names, and raw IPs.
 */
public final class HostUtils {

    public static final Set<String> LOOPBACK_HOSTS = Set.of("localhost", "127.0.0.1", "::1");

    private static final Map<String, String> CACHE = new ConcurrentHashMap<>();

    private HostUtils() {
    }

    /**
     * Normalizes a hostname or URI to an IP address.
     * Results are cached to avoid repeated DNS resolution.
     *
     * @param input hostname, URI, or IP
     * @return resolved IP address, or original input on failure
     */
    public static String normalize(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        // Short-circuit for known loopback addresses
        String hostOnly = input.contains("://") ? URI.create(input).getHost() : input;
        if (hostOnly != null && LOOPBACK_HOSTS.contains(hostOnly)) {
            return "127.0.0.1";
        }

        return CACHE.computeIfAbsent(input, key -> {
            try {
                String host = key;

                if (key.contains("://")) {
                    URI uri = URI.create(key);
                    if (uri.getHost() != null) {
                        host = uri.getHost();
                    }
                }

                return InetAddress.getByName(host).getHostAddress();
            } catch (Exception e) {
                return key;
            }
        });
    }
}
