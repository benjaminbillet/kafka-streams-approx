package kafka.streams.serdes;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class ObjectDeserializer<T> implements Deserializer<T> {
  public static final String OBJECT_DESERIALIZER_CLASS_KEY = "object.deserializer.class";

  private Class<T> clazz;

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    this.clazz = (Class<T>) props.get(OBJECT_DESERIALIZER_CLASS_KEY);
  }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      return clazz.cast(ois.readObject());
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {
    // nothing to close
  }
}
