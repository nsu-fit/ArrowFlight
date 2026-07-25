package net.surpin.data.arrowflight.client.spark;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Verifies isolation of catalog-level exact Arrow schema cache keys.
 */
@Tag("unit")
class FlightSessionCatalogTest {

    /**
     * Verifies local identifiers, credentials, and routing produce isolated opaque keys.
     */
    @Test
    void schemaCacheKeyIsolatesPersistedConnectionIdentity() {
        Identifier first = Identifier.of(new String[]{"default"}, "orders_a");
        Identifier second = Identifier.of(new String[]{"default"}, "orders_b");
        Map<String, String> base = new HashMap<>();
        base.put("host", "flight.internal");
        base.put("port", "32010");
        base.put("table", "sales.orders");
        base.put("user", "alice");
        base.put("password", "top-secret");
        base.put(FlightSource.KEY_ROUTING_QUEUE, "interactive");

        String firstKey = FlightSessionCatalog.schemaCacheKey(
                first, new CaseInsensitiveStringMap(base));
        String repeatedKey = FlightSessionCatalog.schemaCacheKey(
                first, new CaseInsensitiveStringMap(base));
        String secondTableKey = FlightSessionCatalog.schemaCacheKey(
                second, new CaseInsensitiveStringMap(base));
        Map<String, String> otherIdentity = new HashMap<>(base);
        otherIdentity.put("user", "bob");
        otherIdentity.put(FlightSource.KEY_ROUTING_QUEUE, "batch");
        String otherIdentityKey = FlightSessionCatalog.schemaCacheKey(
                first, new CaseInsensitiveStringMap(otherIdentity));

        assertEquals(firstKey, repeatedKey);
        assertNotEquals(firstKey, secondTableKey);
        assertNotEquals(firstKey, otherIdentityKey);
        assertFalse(firstKey.contains("top-secret"));
    }
}
