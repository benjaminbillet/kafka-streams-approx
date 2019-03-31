package kafka.streams.serdes;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class ObjectSerializer<T> implements Serializer<T> {
  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    // nothing to configure
  }

  @Override
  public byte[] serialize(String topic, T data) {
    if (data == null) {
      return null;
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(data);
      return baos.toByteArray();
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {
    // nothing to close
  }
}
