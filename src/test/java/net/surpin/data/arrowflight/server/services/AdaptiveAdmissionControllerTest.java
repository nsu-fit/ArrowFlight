package net.surpin.data.arrowflight.server.services;

import net.surpin.data.arrowflight.server.model.SchedulerConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests bounded local admission and adaptive query concurrency.
 */
@Tag("unit")
class AdaptiveAdmissionControllerTest {

    /**
     * Verifies a queued query starts after the active permit is released.
     *
     * @throws Exception if asynchronous admission fails
     */
    @Test
    void queuedQueryRunsAfterPermitRelease() throws Exception {
        AdaptiveAdmissionController controller =
                new AdaptiveAdmissionController(config(1, 1, 1, 2_000L));
        AdaptiveAdmissionController.Permit first =
                controller.acquire(() -> false);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<AdaptiveAdmissionController.Permit> waiting =
                    executor.submit(() -> controller.acquire(() -> false));
            awaitQueued(controller);

            assertEquals(1, controller.activeQueries());
            assertEquals(1, controller.queuedQueries());
            assertThrows(
                    AdaptiveAdmissionController.AdmissionRejectedException.class,
                    () -> controller.acquire(() -> false));

            first.close();
            try (AdaptiveAdmissionController.Permit second =
                         waiting.get(2, TimeUnit.SECONDS)) {
                assertEquals(1, controller.activeQueries());
                second.markSuccessful(1024L);
            }
            assertEquals(0, controller.activeQueries());
            assertTrue(controller.throughputBytesPerSecond() > 0L);
        } finally {
            first.close();
            executor.shutdownNow();
        }
    }

    /**
     * Verifies a query fails when local queue wait exceeds its deadline.
     *
     * @throws Exception if permit setup fails
     */
    @Test
    void queueWaitTimesOut() throws Exception {
        AdaptiveAdmissionController controller =
                new AdaptiveAdmissionController(config(1, 1, 1, 20L));
        try (AdaptiveAdmissionController.Permit ignored =
                     controller.acquire(() -> false)) {
            assertThrows(
                    AdaptiveAdmissionController.AdmissionRejectedException.class,
                    () -> controller.acquire(() -> false));
        }
    }

    /**
     * Verifies high pressure reduces the limit and low pressure raises it gradually.
     *
     * @throws Exception if permits cannot be acquired
     */
    @Test
    void pressureAdjustsConcurrencyLimit() throws Exception {
        AdaptiveAdmissionController controller =
                new AdaptiveAdmissionController(config(1, 4, 4, 1_000L));

        controller.updatePressure(0.99, 0.50);
        assertEquals(2, controller.concurrencyLimit());

        try (AdaptiveAdmissionController.Permit first =
                     controller.acquire(() -> false);
                AdaptiveAdmissionController.Permit second =
                     controller.acquire(() -> false)) {
            Thread.sleep(2L);
            controller.updatePressure(0.10, 0.10);
            assertEquals(3, controller.concurrencyLimit());
        }
    }

    /**
     * Verifies a queued redirectable request is released for target evaluation.
     *
     * @throws Exception if permit setup fails
     */
    @Test
    void queuedRequestReachesRedirectThreshold() throws Exception {
        SchedulerConfig redirectConfig = new SchedulerConfig(
                true, 100L, 500L, 1L,
                1, 1, 1, 1_000L,
                0.65, 0.90, 0.70, 0.85, 250L,
                true, 5L, 2, 0.30);
        AdaptiveAdmissionController controller =
                new AdaptiveAdmissionController(redirectConfig);

        try (AdaptiveAdmissionController.Permit ignored =
                     controller.acquire(() -> false)) {
            assertThrows(
                    AdaptiveAdmissionController.AdmissionRedirectException.class,
                    () -> controller.acquire(() -> false, true));
        }

        assertEquals(0, controller.queuedQueries());
    }

    private static SchedulerConfig config(
            int minimum, int maximum, int maximumQueued, long waitMillis) {
        return new SchedulerConfig(
                true, 100L, 500L, 1L,
                minimum, maximum, maximumQueued, waitMillis,
                0.65, 0.90, 0.70, 0.85, 250L);
    }

    private static void awaitQueued(AdaptiveAdmissionController controller)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (controller.queuedQueries() == 0
                && System.nanoTime() < deadline) {
            Thread.sleep(1L);
        }
        assertEquals(1, controller.queuedQueries());
    }
}
