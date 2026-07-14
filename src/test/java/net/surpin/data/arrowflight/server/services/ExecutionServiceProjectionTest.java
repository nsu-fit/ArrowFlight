package net.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExecutionServiceProjectionTest {

    @Test
    void projectionIncludesSelectedAndFilterColumnsForSparkScan() {
        ParquetQueryParser query = ParquetQueryParser.parse(
                "select l_returnflag, l_linestatus, l_quantity, l_extendedprice, "
                + "l_discount, l_tax from tpch.lineitem "
                + "where l_shipdate <= '1998-09-02'");

        assertEquals(Set.of(
                "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice",
                "l_discount", "l_tax", "l_shipdate"),
                ExecutionService.projectedColumns(query));
    }

    @Test
    void projectionIncludesQ6SelectedAndFilterColumns() {
        ParquetQueryParser query = ParquetQueryParser.parse(
                "select l_extendedprice, l_discount from tpch.lineitem "
                + "where l_shipdate >= '1994-01-01' "
                + "and l_shipdate < '1995-01-01' "
                + "and l_discount >= 0.05 and l_discount <= 0.07 "
                + "and l_quantity < 24");

        assertEquals(Set.of(
                "l_extendedprice", "l_discount", "l_shipdate", "l_quantity"),
                ExecutionService.projectedColumns(query));
    }
}
