package net.surpin.data.arrowflight.server.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AceroFileResolverTest {

    @TempDir
    java.nio.file.Path temporaryDirectory;

    @Test
    void usesByteIdenticalLocalMirror() throws Exception {
        java.nio.file.Path dataRoot = temporaryDirectory.resolve("hdfs-root");
        java.nio.file.Path source = dataRoot.resolve("tpch/lineitem/part.parquet");
        java.nio.file.Path mirrorRoot = temporaryDirectory.resolve("staging");
        java.nio.file.Path mirror = mirrorRoot.resolve("tpch/lineitem/part.parquet");
        byte[] contents = "parquet-data".getBytes(StandardCharsets.UTF_8);
        Files.createDirectories(source.getParent());
        Files.createDirectories(mirror.getParent());
        Files.write(source, contents);
        Files.write(mirror, contents);

        RawLocalFileSystem fileSystem = localFileSystem();
        FileStatus status = fileSystem.getFileStatus(
                new org.apache.hadoop.fs.Path(source.toUri()));
        AceroFileResolver resolver = new AceroFileResolver(
                fileSystem, new org.apache.hadoop.fs.Path(dataRoot.toUri()),
                mirrorRoot.toString(), 8192, temporaryDirectory.resolve("cache"));

        assertEquals(mirror.toUri().toString(), resolver.resolve(status));
    }

    @Test
    void copiesThroughHadoopApiWhenLocalMirrorIsMissing() throws Exception {
        java.nio.file.Path dataRoot = temporaryDirectory.resolve("hdfs-root");
        java.nio.file.Path source = dataRoot.resolve("tpch/orders/part.parquet");
        byte[] contents = "remote-parquet-data".getBytes(StandardCharsets.UTF_8);
        Files.createDirectories(source.getParent());
        Files.write(source, contents);

        RawLocalFileSystem fileSystem = localFileSystem();
        FileStatus status = fileSystem.getFileStatus(
                new org.apache.hadoop.fs.Path(source.toUri()));
        AceroFileResolver resolver = new AceroFileResolver(
                fileSystem, new org.apache.hadoop.fs.Path(dataRoot.toUri()), null,
                8192, temporaryDirectory.resolve("cache"));

        URI resolvedUri = URI.create(resolver.resolve(status));
        java.nio.file.Path resolved = java.nio.file.Path.of(resolvedUri);
        assertNotEquals(source, resolved);
        assertTrue(resolved.startsWith(temporaryDirectory.resolve("cache")));
        assertEquals("remote-parquet-data", Files.readString(resolved));
    }

    private static RawLocalFileSystem localFileSystem() throws Exception {
        RawLocalFileSystem fileSystem = new RawLocalFileSystem();
        fileSystem.initialize(URI.create("file:///"), new Configuration());
        return fileSystem;
    }
}
