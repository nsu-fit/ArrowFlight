package com.oltpbenchmark.benchmarks.tpcds.procedures;

import com.oltpbenchmark.api.Procedure;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Base class for all TPC-DS query procedures.
 * Mirrors {@link com.oltpbenchmark.benchmarks.tpch.procedures.GenericQuery}.
 */
public abstract class TPCDSProcedure extends Procedure {

    /**
     * Execute this TPC-DS query and drain the result set.
     *
     * @param conn JDBC connection
     * @throws SQLException on query failure
     */
    public abstract void run(Connection conn) throws SQLException;
}
