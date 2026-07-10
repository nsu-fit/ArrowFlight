package net.surpin.data.arrowflight.server;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;

import net.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import net.surpin.data.arrowflight.server.model.AppConfig;

/**
 * Main entry point for the Hadoop Arrow Flight SQL server.
 * Uses Dagger for dependency injection and manages the Flight server lifecycle.
 */
public class HadoopArrowFlightServer {

    static {
        RuntimeSettings.logLevel();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopArrowFlightServer.class);

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
        int hazelcastPort = Integer.parseInt(
                getArgValue(args, "--hazelcast-port", String.valueOf(config.hazelcastPort())));

        // Push CLI overrides into system properties so ConfigAdapter picks them up
        System.setProperty("dataDir", dataDirectory);
        System.setProperty("port", String.valueOf(port));
        System.setProperty("hazelcastPort", String.valueOf(hazelcastPort));

        LOGGER.info("Starting Hadoop Arrow Flight SQL server...");
        LOGGER.info("Data Directory: {}", dataDirectory);
        LOGGER.info("Hosts: {}", hosts);
        LOGGER.info("Port: {}", port);

        Configuration hadoopConfig = new Configuration();
        String[] hazelcastHosts = hosts.split(",");

        // Create Dagger component with runtime parameters
        component = DaggerServerComponent.builder()
                .serverModule(new ServerModule(hazelcastHosts,
                        Location.forGrpcInsecure(localhost, port).getUri().toString(),
                        hadoopConfig))
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

    private void waitForCluster(String[] hosts, int timeoutSec) {
        HazelcastInstance hazelcast = component.clusterService().getHazelcastInstance();
        long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        long started = System.currentTimeMillis();

        LOGGER.info("Waiting up to {}s for {} nodes to connect (hosts: {})",
                timeoutSec, hosts.length, Arrays.toString(hosts));

        Set<Member> members;
        do {
            members = hazelcast.getCluster().getMembers();
            long elapsed = (System.currentTimeMillis() - started) / 1000;
            LOGGER.info("Connected: {} of {} nodes ({}s elapsed)",
                    members.size(), hosts.length, elapsed);

            if (System.currentTimeMillis() >= deadline) {
                String msg = String.format(
                        "Cluster join timeout after %ds: only %d of %d nodes connected. "
                                + "Expected hosts: %s.",
                        timeoutSec, members.size(), hosts.length, Arrays.toString(hosts));
                LOGGER.error(msg);
                throw new IllegalStateException(msg);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for cluster join", e);
            }
        } while (members.size() < hosts.length);

        long totalSec = (System.currentTimeMillis() - started) / 1000;
        LOGGER.info("All {} nodes connected in {}s. Initializing...", hosts.length, totalSec);
    }

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
