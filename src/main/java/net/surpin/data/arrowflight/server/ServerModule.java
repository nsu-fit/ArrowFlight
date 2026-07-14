package net.surpin.data.arrowflight.server;

import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;

import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import net.surpin.data.arrowflight.server.adapters.AceroAdapter;
import net.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import net.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import net.surpin.data.arrowflight.server.adapters.FilterConverter;
import net.surpin.data.arrowflight.server.adapters.FlightSqlProducer;
import net.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import net.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import net.surpin.data.arrowflight.server.services.ParquetQueryParser;
import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.services.ClusterService;
import net.surpin.data.arrowflight.server.services.ExecutionService;
import net.surpin.data.arrowflight.server.services.MetadataService;
import net.surpin.data.arrowflight.server.services.QueryPlanner;

/**
 * Dagger module that wires all server components as singletons.
 * Runtime parameters (hosts, Hadoop config) are passed via constructor.
 */
@Module
public final class ServerModule {

    private final String[] hazelcastHosts;
    private final String serverUri;
    private final String localStorageHost;
    private final Configuration hadoopConfig;

    /**
     * Creates module with runtime parameters.
     *
     * @param hazelcastHosts cluster seed hosts
     * @param serverUri      this server's URI
     * @param localStorageHost HDFS DataNode host colocated with this server
     * @param hadoopConfig   Hadoop configuration
     */
    public ServerModule(String[] hazelcastHosts, String serverUri,
            String localStorageHost, Configuration hadoopConfig) {
        this.hazelcastHosts = hazelcastHosts;
        this.serverUri = serverUri;
        this.localStorageHost = localStorageHost;
        this.hadoopConfig = hadoopConfig;
    }

    /**
     * Provide AppConfig singleton
     * @return application configuration
     */
    @Provides
    @Singleton
    AppConfig config() {
        System.setProperty(
            org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
            org.apache.arrow.memory.DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        return ConfigAdapter.getConfig();
    }

    /**
     * Provide HazelcastAdapter singleton
     * @param config application configuration
     * @return Hazelcast adapter
     */
    @Provides
    @Singleton
    HazelcastAdapter hazelcast(AppConfig config) {
        return new HazelcastAdapter(config, hazelcastHosts);
    }

    /**
     * Provide ClusterService singleton
     * @param hazelcast Hazelcast adapter
     * @param config application configuration
     * @return cluster service
     */
    @Provides
    @Singleton
    ClusterService cluster(HazelcastAdapter hazelcast, AppConfig config) {
        return new ClusterService(hazelcast, config, serverUri);
    }

    /**
     * Provide I/O thread pool singleton
     * @param config application configuration
     * @return executor service
     */
    @Provides
    @Singleton
    ExecutorService ioPool(AppConfig config) {
        return Executors.newFixedThreadPool(config.ioParallelism(), r -> {
            Thread t = new Thread(r, "parquet-io");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Provide Hadoop FileSystem singleton
     * @param config application configuration
     * @return Hadoop file system
     */
    @Provides
    @Singleton
    FileSystem fileSystem(AppConfig config) {
        hadoopConfig.setInt("io.file.buffer.size", config.ioFileBufferSize());
        hadoopConfig.setBoolean("dfs.client.read.shortcircuit", true);
        hadoopConfig.setBoolean("dfs.client.read.shortcircuit.skip.checksum", false);
        hadoopConfig.setBoolean("fs.file.impl.disable.cache", true);
        try {
            return new Path(config.dataDir()).getFileSystem(hadoopConfig);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Provide ParquetAdapter singleton
     * @param config application configuration
     * @param fs Hadoop file system
     * @return parquet adapter
     */
    @Provides
    @Singleton
    ParquetAdapter parquetAdapter(AppConfig config, FileSystem fs) {
        return new ParquetAdapter(config, fs, localStorageHost);
    }

    /**
     * Provide DuckDbAdapter singleton
     * @param config application configuration
     * @param ioPool I/O thread pool
     * @return DuckDB adapter
     */
    @Provides
    @Singleton
    DuckDbAdapter duckDb(AppConfig config, ExecutorService ioPool) {
        return new DuckDbAdapter(config, ioPool);
    }

    /**
     * Provide MetadataService singleton
     * @param parquetAdapter parquet adapter
     * @return metadata service
     */
    @Provides
    @Singleton
    MetadataService metadata(ParquetAdapter parquetAdapter) {
        return new MetadataService(parquetAdapter);
    }

    /**
     * Provide QueryPlanner singleton
     * @param parquetAdapter parquet adapter
     * @param clusterService cluster service
     * @return query planner
     */
    @Provides
    @Singleton
    QueryPlanner queryPlanner(ParquetAdapter parquetAdapter, ClusterService clusterService) {
        return new QueryPlanner(parquetAdapter, clusterService);
    }

    /**
     * Provide AceroAdapter singleton
     * @param config application configuration
     * @return Acero adapter
     */
    @Provides
    @Singleton
    AceroAdapter acero(AppConfig config) {
        return new AceroAdapter(config);
    }

    /**
     * Provide filter builder singleton
     * @param parquetAdapter parquet adapter
     * @return filter builder function
     */
    @Provides
    @Singleton
    Function<ParquetQueryParser, byte[]> filterBuilder(ParquetAdapter parquetAdapter) {
        return pq -> {
            if (pq.filter == null || pq.filter.trim().isEmpty()) {
                return null;
            }
            Map<String, Map<String, String>> ddlCache = parquetAdapter.tableDdlCache();
            String ddl = ddlCache.getOrDefault(pq.schema, Collections.emptyMap())
                    .get(pq.table);
            if (ddl == null) {
                return null;
            }
            String cleanDdl = ddl.replace(pq.schema + ".", "");
            try {
                ByteBuffer bb = FilterConverter.toByteBuffer(
                        pq.filter, Collections.singletonList(cleanDdl));
                byte[] bytes = new byte[bb.remaining()];
                bb.get(bytes);
                return bytes;
            } catch (Exception e) {
                return null;
            }
        };
    }

    /**
     * Provide BufferAllocator singleton
     * @return buffer allocator
     */
    @Provides
    @Singleton
    BufferAllocator allocator() {
        return new RootAllocator(Long.MAX_VALUE);
    }

    /**
     * Provide FlightSqlProducer singleton
     * @param config application configuration
     * @param allocator buffer allocator
     * @param metadataService metadata service
     * @param queryPlanner query planner
     * @param executionService execution service
     * @param clusterService cluster service
     * @return Flight SQL producer
     */
    @Provides
    @Singleton
    FlightSqlProducer producer(AppConfig config, BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService) {
        int port = config.port();
        String localhost = "0.0.0.0";
        Location location = Location.forGrpcInsecure(localhost, port);
        return new FlightSqlProducer(location, allocator, metadataService,
                queryPlanner, executionService, clusterService);
    }

    /**
     * Provide ExecutionService singleton
     * @param parquetAdapter parquet adapter
     * @param duckDbAdapter DuckDB adapter
     * @param aceroAdapter Acero adapter
     * @param metadataService metadata service
     * @param appConfig application configuration
     * @param ioPool I/O thread pool
     * @param filterBuilder filter builder function
     * @return execution service
     */
    @Provides
    @Singleton
    ExecutionService execution(ParquetAdapter parquetAdapter, DuckDbAdapter duckDbAdapter,
            AceroAdapter aceroAdapter, MetadataService metadataService,
            AppConfig appConfig, ExecutorService ioPool,
            Function<ParquetQueryParser, byte[]> filterBuilder) {
        return new ExecutionService(parquetAdapter, duckDbAdapter, aceroAdapter,
                metadataService, appConfig, ioPool, filterBuilder);
    }
}
