package net.surpin.data.arrowflight.server.metrics;

import net.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import net.surpin.data.arrowflight.server.model.ServerCapacity;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests Prometheus metrics collection and HTTP exposition.
 */
@Tag("unit")
class MetricsServiceTest {

    /**
     * Verifies query metrics are exposed with bounded labels.
     *
     * @throws Exception if the local metrics endpoint cannot be queried
     */
    @Test
    void exposesQueryMetrics() throws Exception {
        try (MetricsService.QueryObservation observation = MetricsService.observeQuery(
                "SELECT * FROM tpch.lineitem WHERE l_shipdate > DATE '1998-01-01'", 4096L)) {
            observation.markFailed();
        }
        try (MetricsService service = new MetricsService(0)) {
            service.start();
            HttpResponse<String> response = get(service.port(), "/metrics");

            assertEquals(200, response.statusCode());
            assertTrue(response.headers().firstValue("Content-Type")
                    .orElseThrow().contains("version=0.0.4"));
            assertTrue(response.body().contains(
                    "arrowflight_parquet_queries_total{path=\"filtered-scan\"}"));
            assertTrue(response.body().contains(
                    "arrowflight_parquet_query_failures_total{path=\"filtered-scan\"}"));
            assertTrue(response.body().contains(
                    "arrowflight_parquet_logical_input_bytes_total{path=\"filtered-scan\"}"));
            assertTrue(response.body().contains("arrowflight_jvm_threads_live"));
        }
    }

    /**
     * Verifies the health endpoint reports readiness.
     *
     * @throws Exception if the local metrics endpoint cannot be queried
     */
    @Test
    void exposesHealthEndpoint() throws Exception {
        try (MetricsService service = new MetricsService(0)) {
            service.start();
            HttpResponse<String> response = get(service.port(), "/-/healthy");

            assertEquals(200, response.statusCode());
            assertEquals("ok\n", response.body());
        }
    }

    /**
     * Verifies execution capacity, queue, and memory budget metrics.
     *
     * @throws Exception if the local metrics endpoint cannot be queried
     */
    @Test
    void exposesExecutionAndMemoryMetrics() throws Exception {
        MetricsService.bindConfiguration(ConfigAdapter.getConfig());
        MetricsService.bindCapacitySupplier(
                () -> new ServerCapacity(2, 3, 0, 4, 64, 8L, 4L, 7L));
        MetricsService.recordQueueRejection();
        MetricsService.recordQueueWaitMillis(125L);

        try (MetricsService service = new MetricsService(0)) {
            service.start();
            String body = get(service.port(), "/metrics").body();

            assertTrue(body.contains("arrowflight_execution_slots_active 2"));
            assertTrue(body.contains("arrowflight_execution_queue_depth 3"));
            assertTrue(body.contains(
                    "arrowflight_execution_queue_rejections_total 1"));
            assertTrue(body.contains(
                    "arrowflight_execution_queue_wait_seconds_count 1"));
            assertTrue(body.contains(
                    "arrowflight_query_engine_memory_limit_bytes"));
            assertTrue(body.contains(
                    "arrowflight_duckdb_query_memory_limit_bytes"));
            assertTrue(body.contains(
                    "arrowflight_arrow_shared_reserve_bytes"));
        }
    }

    /**
     * Reads one endpoint from a local metrics server.
     *
     * @param port local HTTP port
     * @param path endpoint path
     * @return HTTP response body and status
     * @throws Exception if the request fails
     */
    private static HttpResponse<String> get(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + port + path)).GET().build();
        return HttpClient.newHttpClient().send(request,
                HttpResponse.BodyHandlers.ofString());
    }
}
