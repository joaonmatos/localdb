package com.localdb.serialization.cbor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.localdb.serialization.SerializationStrategy;
import java.io.IOException;

public class CborSerializationStrategy<T> implements SerializationStrategy<T> {
  private final ObjectMapper mapper;
  private final Class<T> clazz;

  public CborSerializationStrategy(Class<T> clazz) {
    this.clazz = clazz;
    this.mapper = new ObjectMapper(new CBORFactory());
  }

  @Override
  public byte[] serialize(T object) throws IOException {
    return mapper.writeValueAsBytes(object);
  }

  @Override
  public T deserialize(byte[] data) throws IOException {
    return mapper.readValue(data, clazz);
  }

  @Override
  public int getSerializedSize(T object) {
    try {
      return serialize(object).length;
    } catch (IOException e) {
      throw new RuntimeException("Failed to calculate serialized size", e);
    }
  }
}
