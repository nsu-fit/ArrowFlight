package net.surpin.data.arrowflight.server;

import net.surpin.data.arrowflight.server.model.AppConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests server dependency providers. */
@Tag("unit")
class ServerModuleTest {

    /** Verifies that the I/O executor uses bounded, named virtual workers. */
    @Test
    void ioPoolUsesBoundedVirtualThreads() throws Exception {
        AppConfig config = mock(AppConfig.class);
        when(config.ioParallelism()).thenReturn(1);
        ServerModule module = new ServerModule(null, null, null, null);
        ExecutorService executor = module.ioPool(config);
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);

        try {
            Future<Boolean> first = executor.submit(() -> {
                firstStarted.countDown();
                releaseFirst.await();
                return Thread.currentThread().isVirtual()
                        && Thread.currentThread().getName().startsWith("parquet-io-");
            });
            Future<Boolean> second = executor.submit(() -> {
                secondStarted.countDown();
                return Thread.currentThread().isVirtual();
            });

            assertTrue(firstStarted.await(1, TimeUnit.SECONDS));
            assertFalse(secondStarted.await(100, TimeUnit.MILLISECONDS));
            releaseFirst.countDown();
            assertTrue(first.get(1, TimeUnit.SECONDS));
            assertTrue(second.get(1, TimeUnit.SECONDS));
        } finally {
            releaseFirst.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }
}
