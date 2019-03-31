package kafka.streams.serdes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A factory for building Serde based on ObjectOutputStream and
 * ObjectInputStream.
 *
 * This factory is only provided as a convenience for testing, in practice a
 * more portable serialization format (JSON, protobuf, Avro, etc.) should be
 * preferred.
 */
public class ObjectSerdeFactory {
  public static <T> Serde<T> createSerde(Class<T> clazz) {
    return createSerde(clazz, Collections.emptyMap());
  }

  public static <T> Serde<T> createSerde(Class<T> clazz, Map<String, Object> props) {
    Serializer<T> serializer = new ObjectSerializer<>();
    serializer.configure(props, false);

    Map<String, Object> allProps = new HashMap<>(props);
    allProps.put(ObjectDeserializer.OBJECT_DESERIALIZER_CLASS_KEY, clazz);

    Deserializer<T> deserializer = new ObjectDeserializer<>();
    deserializer.configure(allProps, false);

    return Serdes.serdeFrom(serializer, deserializer);
  }
}
