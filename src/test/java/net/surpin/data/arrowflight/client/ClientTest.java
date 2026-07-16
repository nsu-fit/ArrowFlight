package net.surpin.data.arrowflight.client;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ClientTest {

    @AfterEach
    void tearDown() throws Exception {
        clearClients();
    }

    @SuppressWarnings("unchecked")
    private static void clearClients() throws Exception {
        Field field = Client.class.getDeclaredField("clients");
        field.setAccessible(true);
        ConcurrentMap<String, Client> clients = (ConcurrentMap<String, Client>) field.get(null);
        clients.clear();
    }

    private static Configuration config(String host, int port) {
        return new Configuration(host, port, null, null, "test-token");
    }

    @Test
    void getOrCreate_returnsSameClientForSameConfig() {
        try (MockedStatic<FlightClient> flightClientMock = mockStatic(FlightClient.class)) {
            FlightClient.Builder builder = mock(FlightClient.Builder.class);
            FlightClient mockClient = mock(FlightClient.class);

            when(FlightClient.builder()).thenReturn(builder);
            when(builder.allocator(any())).thenReturn(builder);
            when(builder.location(any())).thenReturn(builder);
            when(builder.build()).thenReturn(mockClient);

            Configuration c = config("host1", 32010);
            Client c1 = Client.getOrCreate(c);
            Client c2 = Client.getOrCreate(c);
            assertSame(c1, c2);
            c1.close();
        }
    }

    @Test
    void getOrCreate_differentConfigsDifferentClients() {
        try (MockedStatic<FlightClient> flightClientMock = mockStatic(FlightClient.class)) {
            FlightClient.Builder builder = mock(FlightClient.Builder.class);
            FlightClient mockClient = mock(FlightClient.class);

            when(FlightClient.builder()).thenReturn(builder);
            when(builder.allocator(any())).thenReturn(builder);
            when(builder.location(any())).thenReturn(builder);
            when(builder.build()).thenReturn(mockClient);

            Configuration c1 = config("host1", 32010);
            Configuration c2 = config("host2", 32010);
            Client cl1 = Client.getOrCreate(c1);
            Client cl2 = Client.getOrCreate(c2);
            assertNotSame(cl1, cl2);
            cl1.close();
            cl2.close();
        }
    }

    @Test
    void close_removesClientFromPool() throws Exception {
        try (MockedStatic<FlightClient> flightClientMock = mockStatic(FlightClient.class)) {
            FlightClient.Builder builder = mock(FlightClient.Builder.class);
            FlightClient mockClient = mock(FlightClient.class);

            when(FlightClient.builder()).thenReturn(builder);
            when(builder.allocator(any())).thenReturn(builder);
            when(builder.location(any())).thenReturn(builder);
            when(builder.build()).thenReturn(mockClient);

            Configuration c = config("host-close", 32010);
            Client client = Client.getOrCreate(c);
            client.close();

            Field field = Client.class.getDeclaredField("clients");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            ConcurrentMap<String, Client> clients = (ConcurrentMap<String, Client>) field.get(null);
            assertFalse(clients.containsKey(c.getConnectionString()));
        }
    }

    @Test
    void getQueryEndpoints_returnsEndpointsFromMockClient() throws Exception {
        try (MockedStatic<FlightClient> flightClientMock = mockStatic(FlightClient.class)) {
            FlightClient.Builder builder = mock(FlightClient.Builder.class);
            FlightClient mockClient = mock(FlightClient.class);

            when(FlightClient.builder()).thenReturn(builder);
            when(builder.allocator(any())).thenReturn(builder);
            when(builder.location(any())).thenReturn(builder);
            when(builder.build()).thenReturn(mockClient);

            FlightEndpoint endpoint = new FlightEndpoint(
                    new Ticket(new byte[0]),
                    Location.forGrpcInsecure("server", 32010));
            FlightInfo flightInfo = new FlightInfo(
                    new org.apache.arrow.vector.types.pojo.Schema(List.of()),
                    FlightDescriptor.command(new byte[0]),
                    List.of(endpoint),
                    -1, -1);
            when(mockClient.getInfo(any(FlightDescriptor.class), any(CallOption.class)))
                    .thenReturn(flightInfo);

            Configuration c = config("host-eps", 32010);
            Client client = Client.getOrCreate(c);
            var result = client.getQueryEndpoints("SELECT * FROM t");
            assertEquals(1, result.getEndpoints().length);
            client.close();
        }
    }

    @Test
    void getQueryEndpoints_retriesOnInternalError() throws Exception {
        try (MockedStatic<FlightClient> flightClientMock = mockStatic(FlightClient.class)) {
            FlightClient.Builder builder = mock(FlightClient.Builder.class);
            FlightClient mockClient = mock(FlightClient.class);

            when(FlightClient.builder()).thenReturn(builder);
            when(builder.allocator(any())).thenReturn(builder);
            when(builder.location(any())).thenReturn(builder);
            when(builder.build()).thenReturn(mockClient);

            when(mockClient.getInfo(any(FlightDescriptor.class), any(CallOption.class)))
                    .thenThrow(CallStatus.INTERNAL
                            .withDescription("simulated error").toRuntimeException());

            Configuration c = new Configuration("host1", 32010, null, null, "test-token");
            c.setMaxRetries(1);
            Client client = Client.getOrCreate(c);

            RuntimeException e = assertThrows(RuntimeException.class, () ->
                    client.getQueryEndpoints("SELECT * FROM t"));
            assertTrue(e.getMessage().contains("simulated error"));
            client.close();
        }
    }
}
