package com.oltpbenchmark.benchmarks.tpcds;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.TransactionStatus;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * TPC-DS worker that executes query procedures.
 * Mirrors {@link com.oltpbenchmark.benchmarks.tpch.TPCHWorker}.
 */
public final class TPCDSWorker extends Worker<TPCDSBenchmark> {

    public TPCDSWorker(final TPCDSBenchmark benchmarkModule, final int id) {
        super(benchmarkModule, id);
    }

    @Override
    protected TransactionStatus executeWork(
            final Connection conn,
            final TransactionType nextTransaction)
            throws UserAbortException, SQLException {
        try {
            Procedure proc = this.getProcedure(nextTransaction.getProcedureClass());
            proc.run(conn);
        } catch (ClassCastException e) {
            throw new RuntimeException(e);
        }
        return TransactionStatus.SUCCESS;
    }
}
