package net.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.ByteString;
import net.surpin.data.arrowflight.server.model.HandleState;
import net.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import net.surpin.data.arrowflight.server.model.QueryPlan;
import net.surpin.data.arrowflight.server.model.SchedulerConfig;
import net.surpin.data.arrowflight.server.services.AdaptiveAdmissionController;
import net.surpin.data.arrowflight.server.services.ClusterService;
import net.surpin.data.arrowflight.server.services.ExecutionService;
import net.surpin.data.arrowflight.server.services.MetadataService;
import net.surpin.data.arrowflight.server.services.QueryPlanner;
import net.surpin.data.arrowflight.server.services.TaskRedirectService;
import net.surpin.data.arrowflight.common.FlightRedirectProtocol;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
                "select * from tpch.lineitem", new String[]{"part.parquet"}, null, 0L);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);

        FlightRuntimeException timeout = CallStatus.TIMED_OUT
                .withDescription("listener timeout")
                .toRuntimeException();
        doThrow(timeout).when(executionService).readParquet(
                eq(allocator), eq(state.query()), eq(state.filePaths()), eq(listener),
                anyBoolean(), any(ExecutionPathTracker.class));

        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService, clusterService);
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery
                .newBuilder().setStatementHandle(handle).build();

        producer.getStreamStatement(ticket, context, listener);

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.TIMED_OUT, reported.status().code());
        verify(listener, never()).completed();
    }

    /** Verifies statement planning exports Parquet statistics through FlightInfo. */
    @Test
    void statementFlightInfoContainsQueryStatistics() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        String query = "select * from tpch.lineitem";
        Schema schema = new Schema(List.of());
        when(metadataService.getQuerySchema(query)).thenReturn(schema);
        when(queryPlanner.plan(query)).thenReturn(
                new QueryPlan(List.of(), 4096L, 125L));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService, clusterService);
        FlightSql.CommandStatementQuery command = FlightSql.CommandStatementQuery
                .newBuilder().setQuery(query).build();
        FlightDescriptor descriptor = FlightDescriptor.command(new byte[]{1});

        FlightInfo info = producer.getFlightInfoStatement(
                command, context, descriptor);

        assertEquals(4096L, info.getBytes());
        assertEquals(125L, info.getRecords());
        verify(clusterService, never()).storeHandle(anyString(), any());
    }

    /** Verifies a full local admission queue rejects DoGet before execution. */
    @Test
    void fullAdmissionQueueReturnsResourceExhausted() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        ByteString handle = ByteString.copyFromUtf8("overloaded-query");
        HandleState state = new HandleState(
                "select * from tpch.lineitem",
                new String[]{"part.parquet"}, null, 0L);
        when(clusterService.resolveEndpointHandle(handle.toByteArray()))
                .thenReturn(state);
        SchedulerConfig schedulerConfig = new SchedulerConfig(
                true, 1_000L, 5_000L, 5_000L,
                1, 1, 0, 1_000L,
                0.65, 0.90, 0.70, 0.85, 250L);
        AdaptiveAdmissionController controller =
                new AdaptiveAdmissionController(schedulerConfig);
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService, clusterService,
                controller);
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery
                .newBuilder().setStatementHandle(handle).build();

        try (AdaptiveAdmissionController.Permit ignored =
                     controller.acquire(() -> false)) {
            producer.getStreamStatement(ticket, context, listener);
        }

        ArgumentCaptor<Throwable> error =
                ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED,
                reported.status().code());
        verify(executionService, never()).readParquet(
                any(), anyString(), any(), eq(listener),
                anyBoolean(), any(ExecutionPathTracker.class));
    }

    /** Verifies an overloaded claimed endpoint returns a replacement ticket. */
    @Test
    void redirectedEndpointReturnsFlightMetadata() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        TaskRedirectService redirectService =
                mock(TaskRedirectService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        FlightProducer.CallContext context =
                mock(FlightProducer.CallContext.class);
        ByteString handle = ByteString.copyFromUtf8("redirect-query");
        HandleState state = new HandleState(
                "select * from tpch.lineitem",
                new String[]{"part.parquet"},
                "grpc+tcp://node-1:32010", 100L, true);
        byte[] replacementTicket = new byte[] {9, 8, 7};
        when(clusterService.resolveEndpointHandle(handle.toByteArray()))
                .thenReturn(state);
        when(clusterService.claimEndpointExecution(
                handle.toByteArray(), state)).thenReturn(true);
        when(redirectService.tryRedirect(
                handle.toByteArray(), state)).thenReturn(Optional.of(
                        new TaskRedirectService.Redirect(
                                "grpc+tcp://node-2:32010",
                                replacementTicket, 1)));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService,
                clusterService, AdaptiveAdmissionController.permissive(),
                redirectService);
        FlightSql.TicketStatementQuery ticket =
                FlightSql.TicketStatementQuery.newBuilder()
                        .setStatementHandle(handle).build();

        producer.getStreamStatement(ticket, context, listener);

        ArgumentCaptor<Throwable> error =
                ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        FlightRuntimeException failure = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(
                "grpc+tcp://node-2:32010",
                FlightRedirectProtocol.endpoint(failure)
                        .orElseThrow().getLocations()
                        .getFirst().getUri().toString());
        verify(executionService, never()).readParquet(
                any(), anyString(), any(), eq(listener),
                anyBoolean(), any(ExecutionPathTracker.class));
        verify(clusterService, never()).releaseEndpointLoad(
                handle.toByteArray(), state);
    }
}
