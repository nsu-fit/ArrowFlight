package net.surpin.data.arrowflight.server;

import com.hazelcast.cluster.MembershipAdapter;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.core.HazelcastInstance;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import net.surpin.data.arrowflight.server.model.AppConfig;

/**
 * Main entry point for the Hadoop Arrow Flight SQL server.
 * Uses Dagger for dependency injection and manages the Flight server lifecycle.
 */
public class HadoopArrowFlightServer {

    static {
        initLogLevel();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopArrowFlightServer.class);

    /**
     * Initializes SLF4J log level from properties, system properties, or env var.
     */
    private static void initLogLevel() {
        Properties props = new Properties();
        try (InputStream input = HadoopArrowFlightServer.class.getClassLoader()
                .getResourceAsStream("arrowflight.properties")) {
            if (input != null) {
                props.load(input);
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
        for (String key : new String[]{"logLevel", "arrowflight.log.level"}) {
            String v = System.getProperty(key);
            if (v != null && !v.isBlank()) {
                System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", v.trim());
                return;
            }
            v = props.getProperty(key);
            if (v != null && !v.isBlank()) {
                System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", v.trim());
                return;
            }
        }
        String env = System.getenv("LOGGING_LEVEL");
        if (env != null && !env.isBlank()) {
            System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", env.trim());
        }
    }

    private FlightServer server;
    private ServerComponent component;

    /**
     * Starts the server with command-line arguments.
     *
     * @param args CLI arguments: --data-dir, --port, --hosts, --localhost
     */
    public void start(String... args) {
        AppConfig config = ConfigAdapter.getConfig();
        String dataDirectory = getArgValue(args, "--data-dir", config.dataDir());
        int port = Integer.parseInt(
                getArgValue(args, "--port", String.valueOf(config.port())));
        String hosts = getArgValue(args, "--hosts", "0.0.0.0");
        String localhost = getArgValue(args, "--localhost", "localhost");
        String storageHost = getArgValue(args, "--storage-host", localhost);
        int hazelcastPort = Integer.parseInt(
                getArgValue(args, "--hazelcast-port", String.valueOf(config.hazelcastPort())));

        // Push CLI overrides into system properties so ConfigAdapter picks them up
        System.setProperty("dataDir", dataDirectory);
        System.setProperty("port", String.valueOf(port));
        System.setProperty("hazelcastPort", String.valueOf(hazelcastPort));

        LOGGER.info("Starting Hadoop Arrow Flight SQL server...");
        LOGGER.info("Data Directory: {}", dataDirectory);
        LOGGER.info("Hosts: {}", hosts);
        LOGGER.info("Local storage host: {}", storageHost);
        LOGGER.info("Port: {}", port);

        Configuration hadoopConfig = new Configuration();
        String[] hazelcastHosts = hosts.split(",");

        // Create Dagger component with runtime parameters
        component = DaggerServerComponent.builder()
                .serverModule(new ServerModule(hazelcastHosts,
                        Location.forGrpcInsecure(localhost, port).getUri().toString(),
                        storageHost, hadoopConfig))
                .build();

        // Wait for cluster nodes (single-node skip)
        if (hazelcastHosts.length > 1) {
            waitForCluster(hazelcastHosts, config.hazelcastClusterJoinTimeoutSec());
        }

        try {
            Location location = Location.forGrpcInsecure(localhost, port);
            server = FlightServer.builder(
                    component.allocator(),
                    location,
                    component.producer())
                    .maxInboundMessageSize(config.grpcMaxInboundMessageSize())
                    .build();
            server.start();

            LOGGER.info("Server Arrow Flight SQL started on {}", location);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutting down server...");
                stop();
            }));

            server.awaitTermination();
        } catch (Exception e) {
            LOGGER.error("Error starting server", e);
            System.exit(1);
        }
    }

    /**
     * Stops the server and releases resources.
     */
    public void stop() {
        if (server != null) {
            server.shutdown();
            LOGGER.info("Flight SQL server stopped");
        }
        if (component != null) {
            try {
                component.clusterService().close();
            } catch (Exception e) {
                LOGGER.error("Error closing cluster service", e);
            }
            try {
                component.allocator().close();
            } catch (Exception e) {
                LOGGER.error("Error closing allocator", e);
            }
        }
    }

    /**
     * Waits for all Hazelcast cluster nodes to connect.
     * @param hosts expected host addresses
     * @param timeoutSec max wait time in seconds
     * @throws IllegalStateException if timeout reached or interrupted
     */
    private void waitForCluster(String[] hosts, int timeoutSec) {
        HazelcastInstance hazelcast = component.clusterService().getHazelcastInstance();
        long started = System.currentTimeMillis();

        LOGGER.info("Waiting up to {}s for {} nodes to connect (hosts: {})",
                timeoutSec, hosts.length, Arrays.toString(hosts));

        CountDownLatch latch = new CountDownLatch(hosts.length);
        hazelcast.getCluster().getMembers().forEach(m -> latch.countDown());
        hazelcast.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override public void memberAdded(MembershipEvent e) {
                latch.countDown();
            }
        });

        try {
            if (!latch.await(timeoutSec, TimeUnit.SECONDS)) {
                long elapsed = (System.currentTimeMillis() - started) / 1000;
                int connected = hazelcast.getCluster().getMembers().size();
                String msg = String.format(
                        "Cluster join timeout after %ds: only %d of %d nodes connected. "
                                + "Expected hosts: %s.",
                        elapsed, connected, hosts.length, Arrays.toString(hosts));
                LOGGER.error(msg);
                throw new IllegalStateException(msg);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for cluster join", e);
        }

        long totalSec = (System.currentTimeMillis() - started) / 1000;
        LOGGER.info("All {} nodes connected in {}s. Initializing...", hosts.length, totalSec);
    }

    /**
     * Gets the value of a named CLI argument.
     * @param args CLI arguments array
     * @param key argument name
     * @param defaultValue default if not found
     * @return argument value or default
     */
    private String getArgValue(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(key) && i + 1 < args.length) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    /**
     * Main entry point.
     *
     * @param args CLI arguments
     * @throws Exception on startup failure
     */
    public static void main(String... args) throws Exception {
        HadoopArrowFlightServer server = new HadoopArrowFlightServer();
        server.start(args);
    }
}
