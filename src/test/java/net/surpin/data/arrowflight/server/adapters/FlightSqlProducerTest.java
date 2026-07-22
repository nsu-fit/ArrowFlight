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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.anyBoolean;
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
        when(clusterService.getHandle(handle.toStringUtf8())).thenReturn(state);

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
        verify(listener).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.TIMED_OUT, reported.status().code());
        verify(listener, never()).completed();
    }
}
