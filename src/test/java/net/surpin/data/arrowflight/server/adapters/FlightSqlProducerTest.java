package net.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.ByteString;
import net.surpin.data.arrowflight.server.model.HandleState;
import net.surpin.data.arrowflight.server.services.ClusterService;
import net.surpin.data.arrowflight.server.services.ExecutionService;
import net.surpin.data.arrowflight.server.services.MetadataService;
import net.surpin.data.arrowflight.server.services.QueryPlanner;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;

/** Tests terminal Flight stream status handling in the SQL producer. */
@Tag("unit")
class FlightSqlProducerTest {

    /** Verifies a backpressure timeout is reported as an error, never as completion. */
    @Test
    void timeoutDoesNotCompleteTruncatedStream() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        ByteString handle = ByteString.copyFromUtf8("timeout-query");
        HandleState state = new HandleState(
                "select * from tpch.lineitem",
                new String[]{"part.parquet"}, null, 0L, null);
        when(clusterService.getHandle(handle.toStringUtf8())).thenReturn(state);
        when(allocator.newChildAllocator(anyString(), anyLong(), anyLong()))
                .thenReturn(allocator);

        FlightRuntimeException timeout = CallStatus.TIMED_OUT
                .withDescription("listener timeout")
                .toRuntimeException();
        doThrow(timeout).when(executionService).readParquet(
                eq(allocator), eq(state.query()), eq(state.filePaths()), eq(listener),
                anyBoolean());

        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService, clusterService);
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery
                .newBuilder().setStatementHandle(handle).build();

        producer.getStreamStatement(ticket, context, listener);

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener, timeout(1000)).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.TIMED_OUT, reported.status().code());
        verify(listener, never()).completed();
        producer.close();
    }

    /**
     * Verifies DoGet returns while execution continues on the query executor.
     *
     * @throws Exception on executor shutdown
     */
    @Test
    void doGetDoesNotBlockCallingWorker() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        ByteString handle = ByteString.copyFromUtf8("async-query");
        HandleState state = new HandleState(
                "select * from s.t", new String[]{"f.parquet"}, null, 0L, null);
        when(clusterService.getHandle(handle.toStringUtf8())).thenReturn(state);
        when(allocator.newChildAllocator(anyString(), anyLong(), anyLong()))
                .thenReturn(allocator);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        org.mockito.Mockito.doAnswer(invocation -> {
            started.countDown();
            release.await(1, TimeUnit.SECONDS);
            return null;
        }).when(executionService).readParquet(
                eq(allocator), eq(state.query()), eq(state.filePaths()),
                eq(listener), anyBoolean());

        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService, clusterService);
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery
                .newBuilder().setStatementHandle(handle).build();

        assertTimeoutPreemptively(Duration.ofMillis(250),
                () -> producer.getStreamStatement(
                        ticket, mock(FlightProducer.CallContext.class), listener));
        assertTrue(started.await(1, TimeUnit.SECONDS));
        release.countDown();
        verify(listener, timeout(1000)).completed();
        producer.close();
    }

    /**
     * Verifies DuckDB textual out-of-memory errors use retryable resource status.
     *
     * @throws Exception on executor shutdown
     */
    @Test
    void duckDbOutOfMemoryMapsToResourceExhausted() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        ByteString handle = ByteString.copyFromUtf8("oom-query");
        HandleState state = new HandleState(
                "select * from s.t", new String[]{"f.parquet"}, null, 0L, null);
        when(clusterService.getHandle(handle.toStringUtf8())).thenReturn(state);
        when(allocator.newChildAllocator(anyString(), anyLong(), anyLong()))
                .thenReturn(allocator);
        doThrow(new SQLException("Out of Memory Error: failed to allocate block"))
                .when(executionService).readParquet(
                        eq(allocator), eq(state.query()), eq(state.filePaths()),
                        eq(listener), anyBoolean());

        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService, clusterService);
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery
                .newBuilder().setStatementHandle(handle).build();
        producer.getStreamStatement(
                ticket, mock(FlightProducer.CallContext.class), listener);

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener, timeout(1000)).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED,
                reported.status().code());
        producer.close();
    }
}
