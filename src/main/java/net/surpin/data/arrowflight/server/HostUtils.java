package net.surpin.data.arrowflight.server;

import java.net.InetAddress;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class HostUtils {

    public static final Set<String> LOOPBACK_HOSTS = Set.of("localhost", "127.0.0.1", "::1");

    private static final Map<String, String> CACHE = new ConcurrentHashMap<>();

    /**
     * Normalizes a host or URI to an IP address.
     *
     * <p>Accepts:
     * <ul>
     *   <li>Server URI: "grpc://192.168.1.10:32010" → "192.168.1.10"</li>
     *   <li>URI with domain: "grpc://node1.cluster.local:32010" → "192.168.1.10"</li>
     *   <li>Domain name: "node1.cluster.local" → "192.168.1.10"</li>
     *   <li>IP address: "192.168.1.10" → "192.168.1.10"</li>
     * </ul>
     *
     * @param input host or URI to normalize
     * @return IP address as a string, or the original string if an error occurs
     */
    public static String normalize(String input) {
        if (input == null || input.isEmpty()) {
            return input;
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
