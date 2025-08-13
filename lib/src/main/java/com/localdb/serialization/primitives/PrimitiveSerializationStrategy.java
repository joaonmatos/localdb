package com.localdb.serialization.primitives;

import com.localdb.serialization.SerializationStrategy;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class PrimitiveSerializationStrategy<T> implements SerializationStrategy<T> {

  public static final SerializationStrategy<Integer> INTEGER =
      new SerializationStrategy<Integer>() {
        @Override
        public byte[] serialize(Integer value) throws IOException {
          return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
        }

        @Override
        public Integer deserialize(byte[] data) throws IOException {
          if (data.length != Integer.BYTES) {
            throw new IOException("Invalid data length for integer: " + data.length);
          }
          return ByteBuffer.wrap(data).getInt();
        }

        @Override
        public int getSerializedSize(Integer value) {
          return Integer.BYTES;
        }
      };

  public static final SerializationStrategy<Long> LONG =
      new SerializationStrategy<Long>() {
        @Override
        public byte[] serialize(Long value) throws IOException {
          return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        }

        @Override
        public Long deserialize(byte[] data) throws IOException {
          if (data.length != Long.BYTES) {
            throw new IOException("Invalid data length for long: " + data.length);
          }
          return ByteBuffer.wrap(data).getLong();
        }

        @Override
        public int getSerializedSize(Long value) {
          return Long.BYTES;
        }
      };

  public static final SerializationStrategy<String> STRING =
      new SerializationStrategy<String>() {
        @Override
        public byte[] serialize(String value) throws IOException {
          byte[] stringBytes = value.getBytes("UTF-8");
          ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + stringBytes.length);
          buffer.putInt(stringBytes.length);
          buffer.put(stringBytes);
          return buffer.array();
        }

        @Override
        public String deserialize(byte[] data) throws IOException {
          if (data.length < Integer.BYTES) {
            throw new IOException("Invalid data length for string: " + data.length);
          }
          ByteBuffer buffer = ByteBuffer.wrap(data);
          int stringLength = buffer.getInt();
          if (data.length != Integer.BYTES + stringLength) {
            throw new IOException("Invalid string data length");
          }
          byte[] stringBytes = new byte[stringLength];
          buffer.get(stringBytes);
          return new String(stringBytes, "UTF-8");
        }

        @Override
        public int getSerializedSize(String value) {
          try {
            return Integer.BYTES + value.getBytes("UTF-8").length;
          } catch (Exception e) {
            throw new RuntimeException("Failed to calculate string size", e);
          }
        }
      };

  public static final SerializationStrategy<Double> DOUBLE =
      new SerializationStrategy<Double>() {
        @Override
        public byte[] serialize(Double value) throws IOException {
          return ByteBuffer.allocate(Double.BYTES).putDouble(value).array();
        }

        @Override
        public Double deserialize(byte[] data) throws IOException {
          if (data.length != Double.BYTES) {
            throw new IOException("Invalid data length for double: " + data.length);
          }
          return ByteBuffer.wrap(data).getDouble();
        }

        @Override
        public int getSerializedSize(Double value) {
          return Double.BYTES;
        }
      };

  public static final SerializationStrategy<byte[]> BYTE_ARRAY =
      new SerializationStrategy<byte[]>() {
        @Override
        public byte[] serialize(byte[] value) throws IOException {
          ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + value.length);
          buffer.putInt(value.length);
          buffer.put(value);
          return buffer.array();
        }

        @Override
        public byte[] deserialize(byte[] data) throws IOException {
          if (data.length < Integer.BYTES) {
            throw new IOException("Invalid data length for byte array: " + data.length);
          }
          ByteBuffer buffer = ByteBuffer.wrap(data);
          int arrayLength = buffer.getInt();
          if (data.length != Integer.BYTES + arrayLength) {
            throw new IOException("Invalid byte array data length");
          }
          byte[] result = new byte[arrayLength];
          buffer.get(result);
          return result;
        }

        @Override
        public int getSerializedSize(byte[] value) {
          return Integer.BYTES + value.length;
        }
      };
}
