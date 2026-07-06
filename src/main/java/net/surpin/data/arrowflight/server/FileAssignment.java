package net.surpin.data.arrowflight.server;

import java.util.Set;

public record FileAssignment(long size, Set<String> hosts) {}
