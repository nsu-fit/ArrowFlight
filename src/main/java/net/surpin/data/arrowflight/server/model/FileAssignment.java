package net.surpin.data.arrowflight.server.model;

import java.util.Set;

public record FileAssignment(long size, Set<String> hosts) {}
