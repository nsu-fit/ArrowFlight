package net.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class FlightSqlProducerTest {

    private static TestFlightServerHelper helper;

    @BeforeAll
    static void startServer() throws Exception {
        helper = TestFlightServerHelper.builder().start();
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (helper != null) helper.close();
    }

    @Test
    void getFlightInfoStatementReturnsEndpoints() throws Exception {
        FlightInfo info = helper.sqlClient().execute(
                "SELECT * FROM test_schema.test_table");
        assertNotNull(info);
        assertFalse(info.getEndpoints().isEmpty(),
                "Query must return at least one endpoint");
        assertFalse(info.getSchema().getFields().isEmpty(),
                "Schema must be non-empty");
    }

    @Test
    void getSchemaStatementReturnsCorrectSchema() throws Exception {
        SchemaResult schema = helper.sqlClient().getExecuteSchema(
                "SELECT id, int_col, double_col FROM test_schema.test_table");
        assertNotNull(schema);
        assertEquals(3, schema.getSchema().getFields().size());
    }

    @Test
    void getStreamStatementDeliversRows() throws Exception {
        FlightInfo info = helper.sqlClient().execute(
                "SELECT * FROM test_schema.test_table");
        assertFalse(info.getEndpoints().isEmpty());

        int totalRows = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
            }
        }
        assertTrue(totalRows > 0, "getStream must deliver rows");
    }

    @Test
    void getStreamTablesReturnsTestTable() throws Exception {
        FlightInfo info = helper.sqlClient().getTables(null, null, null, null, false);

        List<String> tableNames = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    var nameVec = root.getVector("table_name");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        Object name = nameVec.getObject(i);
                        if (name != null) tableNames.add(name.toString());
                    }
                }
            }
        }
        assertTrue(tableNames.contains("test_table"),
                "Tables list must contain test_table, got: " + tableNames);
    }
}
