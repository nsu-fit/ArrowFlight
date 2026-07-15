package net.surpin.data.arrowflight.server.services;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import net.surpin.data.arrowflight.server.LogUtil;

/**
 * Resolves Hadoop files to local URIs that Arrow Dataset JNI can open.
 *
 * <p>The Arrow Dataset native library distributed in arrow-dataset 18.0.0 is
 * built without HDFS support. Benchmark nodes already have a byte-identical
 * local staging copy of their HDFS shards, so that copy is preferred. If a
 * shard is not available locally, it is downloaded through the Java Hadoop
 * client into a versioned local cache.</p>
 */
final class AceroFileResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(AceroFileResolver.class);
    private static final String CACHE_DIRECTORY = "arrowflight-acero-cache";

    private final FileSystem fileSystem;
    private final URI qualifiedDataRoot;
    private final java.nio.file.Path localDataRoot;
    private final java.nio.file.Path cacheRoot;
    private final int bufferSize;
    private final Map<String, Object> copyLocks = new ConcurrentHashMap<>();

    AceroFileResolver(FileSystem fileSystem, Path dataRoot, String localDataDir, int bufferSize) {
        this(fileSystem, dataRoot, localDataDir, bufferSize,
                java.nio.file.Path.of(System.getProperty("java.io.tmpdir"), CACHE_DIRECTORY));
    }

    AceroFileResolver(FileSystem fileSystem, Path dataRoot, String localDataDir, int bufferSize,
            java.nio.file.Path cacheBase) {
        this.fileSystem = fileSystem;
        this.qualifiedDataRoot = fileSystem.makeQualified(dataRoot).toUri().normalize();
        this.localDataRoot = localDataDir == null || localDataDir.isBlank()
                ? null : java.nio.file.Path.of(localDataDir).toAbsolutePath().normalize();
        String namespace = UUID.nameUUIDFromBytes(qualifiedDataRoot.toString()
                .getBytes(StandardCharsets.UTF_8)).toString();
        this.cacheRoot = cacheBase.toAbsolutePath().normalize().resolve(namespace);
        this.bufferSize = Math.max(8192, bufferSize);
    }

    /**
     * Returns a local file URI for the supplied Hadoop file.
     *
     * @param status source file status
     * @return local file URI accepted by Arrow Dataset JNI
     * @throws IOException if the source cannot be mapped or copied
     */
    String resolve(FileStatus status) throws IOException {
        long startNanos = System.nanoTime();
        String qid = LogUtil.qid();
        String relative = relativePath(status.getPath());

        if (localDataRoot != null) {
            java.nio.file.Path local = safeResolve(localDataRoot, relative);
            if (isExpectedFile(local, status.getLen())) {
                LOGGER.debug("qid={} node={} resolve=localMirror source={} localPath={} size={} elapsed={}",
                        qid, LogUtil.node(), status.getPath(), local,
                        status.getLen(), LogUtil.elapsedNanos(startNanos));
                return local.toUri().toString();
            }
            if (Files.exists(local)) {
                LOGGER.warn("qid={} node={} resolve=staleMirror source={} localPath={} expected={} found={}",
                        qid, LogUtil.node(), status.getPath(), local,
                        status.getLen(), Files.size(local));
            }
        }

        java.nio.file.Path versionRoot = cacheRoot.resolve(
                status.getModificationTime() + "-" + status.getLen());
        java.nio.file.Path cached = safeResolve(versionRoot, relative);
        Object lock = new Object();
        Object existing = copyLocks.putIfAbsent(cached.toString(), lock);
        Object actual = existing != null ? existing : lock;
        synchronized (actual) {
            if (!isExpectedFile(cached, status.getLen())) {
                copyToCache(status, cached);
            }
        }
        copyLocks.remove(cached.toString(), lock);
        LOGGER.debug("qid={} node={} resolve=cached source={} cached={} size={} modTime={} elapsed={}",
                qid, LogUtil.node(), status.getPath(), cached,
                status.getLen(), status.getModificationTime(),
                LogUtil.elapsedNanos(startNanos));
        return cached.toUri().toString();
    }

    private void copyToCache(FileStatus status, java.nio.file.Path destination)
            throws IOException {
        java.nio.file.Path destinationParent = destination.getParent();
        if (destinationParent == null) {
            throw new IOException("Cache destination has no parent directory: " + destination);
        }
        Files.createDirectories(destinationParent);
        java.nio.file.Path temporary = Files.createTempFile(
                destinationParent, ".arrowflight-", ".part");
        LOGGER.info("qid={} node={} resolve=materializing source={} destination={} size={}",
                LogUtil.qid(), LogUtil.node(), status.getPath(), destination, status.getLen());
        try {
            try (InputStream input = fileSystem.open(status.getPath(), bufferSize);
                    OutputStream output = new BufferedOutputStream(
                            Files.newOutputStream(temporary), bufferSize)) {
                byte[] buffer = new byte[bufferSize];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    if (read > 0) {
                        output.write(buffer, 0, read);
                    }
                }
            }
            long copiedLength = Files.size(temporary);
            if (copiedLength != status.getLen()) {
                throw new IOException("Incomplete Hadoop file copy for " + status.getPath()
                        + ": expected " + status.getLen() + " bytes, copied " + copiedLength);
            }
            moveIntoPlace(temporary, destination);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static void moveIntoPlace(java.nio.file.Path source,
            java.nio.file.Path destination) throws IOException {
        try {
            Files.move(source, destination, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private String relativePath(Path source) throws IOException {
        URI qualifiedSource = fileSystem.makeQualified(source).toUri().normalize();
        if (!sameLocation(qualifiedDataRoot, qualifiedSource)) {
            throw new IOException("Hadoop file is outside configured data directory: " + source);
        }

        String rootPath = qualifiedDataRoot.getPath();
        if (!rootPath.endsWith("/")) {
            rootPath += "/";
        }
        String sourcePath = qualifiedSource.getPath();
        if (!sourcePath.startsWith(rootPath) || sourcePath.length() == rootPath.length()) {
            throw new IOException("Hadoop file is outside configured data directory: " + source);
        }
        return sourcePath.substring(rootPath.length());
    }

    private static boolean sameLocation(URI root, URI source) {
        return java.util.Objects.equals(root.getScheme(), source.getScheme())
                && java.util.Objects.equals(root.getAuthority(), source.getAuthority());
    }

    private static java.nio.file.Path safeResolve(java.nio.file.Path root, String relative)
            throws IOException {
        java.nio.file.Path resolved = root.resolve(relative).normalize();
        if (!resolved.startsWith(root)) {
            throw new IOException("Unsafe relative Hadoop path: " + relative);
        }
        return resolved;
    }

    private static boolean isExpectedFile(java.nio.file.Path file, long expectedLength)
            throws IOException {
        return Files.isRegularFile(file) && Files.size(file) == expectedLength;
    }
}
