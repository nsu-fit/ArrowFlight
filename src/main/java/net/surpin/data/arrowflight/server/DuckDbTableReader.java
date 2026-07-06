package net.surpin.data.arrowflight.server;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Abstraction over how table data is fed to DuckDB for query processing.
 *
 * Two implementations:
 * <ul>
 *   <li>{@link Acero} — current: Acero scans Parquet, exports Arrow C stream, registers in DuckDB</li>
 *   <li>{@link NativeParquet} — future: DuckDB reads Parquet directly via {@code read_parquet()}</li>
 * </ul>
 */
public sealed interface DuckDbTableReader extends AutoCloseable
    permits DuckDbTableReader.Acero, DuckDbTableReader.NativeParquet {

    void register(String name, DuckDBConnection conn) throws Exception;

    @Override
    void close() throws Exception;

    static Acero createAcero(BufferAllocator allocator, List<String> fileUris,
            byte[] filterBytes, Optional<String[]> columns) {
        return new Acero(allocator, fileUris, filterBytes, columns);
    }

    static NativeParquet createNative(List<String> fileUris) {
        return new NativeParquet(fileUris);
    }

    final class Acero implements DuckDbTableReader {

        private final BufferAllocator allocator;
        private final List<String> fileUris;
        private final byte[] filterBytes;
        private final Optional<String[]> columns;

        private FileSystemDatasetFactory factory;
        private Dataset dataset;
        private Scanner scanner;
        private ArrowReader reader;
        private ArrowArrayStream stream;
        private boolean registered;

        Acero(BufferAllocator allocator, List<String> fileUris,
                byte[] filterBytes, Optional<String[]> columns) {
            this.allocator = allocator;
            this.fileUris = Collections.unmodifiableList(fileUris);
            this.filterBytes = filterBytes;
            this.columns = columns;
        }

        @Override
        public void register(String name, DuckDBConnection conn) throws Exception {
            if (registered) throw new IllegalStateException("Already registered: " + name);
            registered = true;

            factory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET,
                    fileUris.toArray(new String[0]));
            dataset = factory.finish();

            ScanOptions.Builder optBuilder = new ScanOptions.Builder(65536).columns(columns);
            if (filterBytes != null) {
                ByteBuffer direct = ByteBuffer.allocateDirect(filterBytes.length);
                direct.put(filterBytes).flip();
                optBuilder.substraitFilter(direct);
            }
            scanner = dataset.newScan(optBuilder.build());
            reader = scanner.scanBatches();
            stream = ArrowArrayStream.allocateNew(allocator);
            Data.exportArrayStream(allocator, reader, stream);
            conn.registerArrowStream(name, stream);
        }

        @Override
        public void close() throws Exception {
            if (stream != null) try { stream.close(); } catch (Exception ignored) {}
            if (reader != null) try { reader.close(); } catch (Exception ignored) {}
            if (scanner != null) try { scanner.close(); } catch (Exception ignored) {}
            if (dataset != null) try { dataset.close(); } catch (Exception ignored) {}
            if (factory != null) try { factory.close(); } catch (Exception ignored) {}
        }
    }

    final class NativeParquet implements DuckDbTableReader {

        private final List<String> fileUris;
        private boolean registered;

        NativeParquet(List<String> fileUris) {
            this.fileUris = Collections.unmodifiableList(fileUris);
        }

        @Override
        public void register(String name, DuckDBConnection conn) throws Exception {
            if (registered) throw new IllegalStateException("Already registered: " + name);
            registered = true;

            StringBuilder uris = new StringBuilder();
            for (int i = 0; i < fileUris.size(); i++) {
                if (i > 0) uris.append(", ");
                uris.append('\'').append(fileUris.get(i)).append('\'');
            }
            String sql = "CREATE OR REPLACE VIEW \"" + name + "\" AS "
                    + "SELECT * FROM read_parquet([" + uris + "])";
            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
        }

        @Override
        public void close() throws Exception {
            // DuckDB views are connection-scoped.
        }
    }
}
