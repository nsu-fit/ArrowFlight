package flight;

import net.surpin.data.arrowflight.client.spark.FlightSource;

/**
 * Spark's provider-name fallback for USING flight when ServiceLoader resources
 * are not visible to Hive Thrift Server session threads.
 */
public class DefaultSource extends FlightSource {
}
