package com.localdb.serialization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class PrimitiveSerializationStrategyTest {

  @Test
  void testIntegerSerialization() throws IOException {
    SerializationStrategy<Integer> strategy = PrimitiveSerializationStrategy.INTEGER;

    Integer value = 42;
    byte[] serialized = strategy.serialize(value);
    Integer deserialized = strategy.deserialize(serialized);

    assertEquals(value, deserialized);
    assertEquals(Integer.BYTES, strategy.getSerializedSize(value));
    assertEquals(Integer.BYTES, serialized.length);
  }

  @Test
  void testLongSerialization() throws IOException {
    SerializationStrategy<Long> strategy = PrimitiveSerializationStrategy.LONG;

    Long value = 123456789L;
    byte[] serialized = strategy.serialize(value);
    Long deserialized = strategy.deserialize(serialized);

    assertEquals(value, deserialized);
    assertEquals(Long.BYTES, strategy.getSerializedSize(value));
    assertEquals(Long.BYTES, serialized.length);
  }

  @Test
  void testStringSerialization() throws IOException {
    SerializationStrategy<String> strategy = PrimitiveSerializationStrategy.STRING;

    String value = "Hello, World!";
    byte[] serialized = strategy.serialize(value);
    String deserialized = strategy.deserialize(serialized);

    assertEquals(value, deserialized);
    assertEquals(Integer.BYTES + value.getBytes("UTF-8").length, strategy.getSerializedSize(value));
  }

  @Test
  void testEmptyStringSerialization() throws IOException {
    SerializationStrategy<String> strategy = PrimitiveSerializationStrategy.STRING;

    String value = "";
    byte[] serialized = strategy.serialize(value);
    String deserialized = strategy.deserialize(serialized);

    assertEquals(value, deserialized);
    assertEquals(Integer.BYTES, strategy.getSerializedSize(value));
  }

  @Test
  void testDoubleSerialization() throws IOException {
    SerializationStrategy<Double> strategy = PrimitiveSerializationStrategy.DOUBLE;

    Double value = 3.14159;
    byte[] serialized = strategy.serialize(value);
    Double deserialized = strategy.deserialize(serialized);

    assertEquals(value, deserialized);
    assertEquals(Double.BYTES, strategy.getSerializedSize(value));
    assertEquals(Double.BYTES, serialized.length);
  }

  @Test
  void testByteArraySerialization() throws IOException {
    SerializationStrategy<byte[]> strategy = PrimitiveSerializationStrategy.BYTE_ARRAY;

    byte[] value = {1, 2, 3, 4, 5};
    byte[] serialized = strategy.serialize(value);
    byte[] deserialized = strategy.deserialize(serialized);

    assertArrayEquals(value, deserialized);
    assertEquals(Integer.BYTES + value.length, strategy.getSerializedSize(value));
  }

  @Test
  void testInvalidIntegerDeserialization() {
    SerializationStrategy<Integer> strategy = PrimitiveSerializationStrategy.INTEGER;

    byte[] invalidData = {1, 2, 3}; // Invalid length
    assertThrows(IOException.class, () -> strategy.deserialize(invalidData));
  }

  @Test
  void testInvalidStringDeserialization() {
    SerializationStrategy<String> strategy = PrimitiveSerializationStrategy.STRING;

    byte[] invalidData = {0, 0, 0, 10, 1, 2}; // Claims 10 bytes but only has 2
    assertThrows(IOException.class, () -> strategy.deserialize(invalidData));
  }
}
