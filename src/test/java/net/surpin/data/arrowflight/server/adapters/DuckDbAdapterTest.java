package net.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.FlightProducer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import net.surpin.data.arrowflight.server.model.AppConfig;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests DuckDB SQL helpers, connection setup, and Flight backpressure handling. */
@Tag("unit")
class DuckDbAdapterTest {

    @Test
    void sqlStringLiteralNormal() {
        assertEquals("'hello'", DuckDbAdapter.sqlStringLiteral("hello"));
    }

    @Test
    void sqlStringLiteralEscapesSingleQuote() {
        assertEquals("'it''s'", DuckDbAdapter.sqlStringLiteral("it's"));
    }

    @Test
    void sqlStringLiteralEmpty() {
        assertEquals("''", DuckDbAdapter.sqlStringLiteral(""));
    }

    @Test
    void quoteIdentifierNormal() {
        assertEquals("\"table_name\"", DuckDbAdapter.quoteIdentifier("table_name"));
    }

    @Test
    void quoteIdentifierEscapesDoubleQuote() {
        assertEquals("\"col\"\"name\"", DuckDbAdapter.quoteIdentifier("col\"name"));
    }

    @Test
    void readParquetFromClauseSingleFile() {
        String result = DuckDbAdapter.readParquetFromClause(List.of("/data/file.parquet"));
        assertEquals("read_parquet(['/data/file.parquet'])", result);
    }

    @Test
    void readParquetFromClauseMultipleFiles() {
        String result = DuckDbAdapter.readParquetFromClause(
                List.of("/data/a.parquet", "/data/b.parquet"));
        assertEquals("read_parquet(['/data/a.parquet', '/data/b.parquet'])", result);
    }

    @Test
    void readParquetFromClauseEmpty() {
        String result = DuckDbAdapter.readParquetFromClause(List.of());
        assertEquals("read_parquet([])", result);
    }

    /** Verifies readiness changes cannot be lost before handler registration. */
    @Test
    void listenerReadinessChangeDoesNotWaitForTimeout() throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean();
        when(listener.isReady()).thenAnswer(invocation -> ready.get());
        doAnswer(invocation -> {
            ready.set(true);
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        assertTimeoutPreemptively(Duration.ofSeconds(1),
                () -> assertTrue(DuckDbAdapter.awaitListenerReady(listener, 200)));
        verify(listener).setOnReadyHandler(any(Runnable.class));
    }

    /** Verifies a readiness signal received before waiting remains observable. */
    @Test
    void listenerReadinessSignalBeforeWaitIsRetained() {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean();
        AtomicInteger readinessChecks = new AtomicInteger();
        when(listener.isReady()).thenAnswer(invocation ->
                readinessChecks.getAndIncrement() > 0 && ready.get());
        doAnswer(invocation -> {
            ready.set(true);
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertTrue(DuckDbAdapter.awaitListenerReady(listener, 200)));
    }

    /** Verifies a spurious wake-up cannot release a non-ready listener. */
    @Test
    void listenerSpuriousWakeupKeepsWaiting() throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean();
        AtomicReference<Runnable> readyHandler = new AtomicReference<>();
        CountDownLatch handlerRegistered = new CountDownLatch(1);
        when(listener.isReady()).thenAnswer(invocation -> ready.get());
        doAnswer(invocation -> {
            readyHandler.set(invocation.getArgument(0));
            handlerRegistered.countDown();
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> result = executor.submit(
                    () -> DuckDbAdapter.awaitListenerReady(listener, 500));
            assertTrue(handlerRegistered.await(1, TimeUnit.SECONDS));
            readyHandler.get().run();
            assertThrows(TimeoutException.class,
                    () -> result.get(50, TimeUnit.MILLISECONDS));

            ready.set(true);
            readyHandler.get().run();
            assertTrue(result.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies cancellation releases a signalled waiter without sending data. */
    @Test
    void listenerCancellationStopsWaiting() throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicReference<Runnable> cancelHandler = new AtomicReference<>();
        CountDownLatch cancelHandlerRegistered = new CountDownLatch(1);
        when(listener.isCancelled()).thenAnswer(invocation -> cancelled.get());
        doAnswer(invocation -> {
            cancelHandler.set(invocation.getArgument(0));
            cancelHandlerRegistered.countDown();
            return null;
        }).when(listener).setOnCancelHandler(any(Runnable.class));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> result = executor.submit(
                    () -> DuckDbAdapter.awaitListenerReady(listener, 500));
            assertTrue(cancelHandlerRegistered.await(1, TimeUnit.SECONDS));
            cancelled.set(true);
            cancelHandler.get().run();
            assertFalse(result.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies an absent readiness signal is bounded by configured timeout. */
    @Test
    void listenerWithoutSignalTimesOut() {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);

        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertFalse(DuckDbAdapter.awaitListenerReady(listener, 25)));
    }

    /** Verifies non-positive readiness timeouts fail before waiting. */
    @Test
    void listenerRejectsNonPositiveTimeout() {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);

        assertThrows(IllegalArgumentException.class,
                () -> DuckDbAdapter.awaitListenerReady(listener, 0));
    }

    @Test
    void ignoresHdfsOptionsWhenExtensionIsNotConfigured() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                4096, 1, 131072, 1, 1, 1,
                null, false, null, null,
                "true", "/var/lib/hadoop-hdfs/socket/dn_socket",
                1048576, 60000L, "/data/parquet", null, 32010, 5701, 60,
                3, 1000, 30000);

        try {
            DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool);
            try (Connection connection = adapter.connection()) {
                assertFalse(connection.isClosed());
            }
        } finally {
            ioPool.shutdownNow();
        }
    }
}
