package com.localdb.serialization;

import java.io.IOException;

/**
 * Strategy interface for serializing and deserializing objects to/from byte arrays. This interface
 * allows the database to work with different serialization formats and custom object types by
 * providing pluggable serialization strategies.
 *
 * <p>Implementations must ensure that:
 *
 * <ul>
 *   <li>Serialization and deserialization are symmetric (round-trip safe)
 *   <li>The same object always serializes to the same byte array
 *   <li>Null objects are handled appropriately
 *   <li>Size calculations are accurate for storage optimization
 * </ul>
 *
 * @param <T> the type of objects this strategy can serialize/deserialize
 */
public interface SerializationStrategy<T> {
  /**
   * Serializes an object to a byte array.
   *
   * @param object the object to serialize
   * @return the serialized object as a byte array
   * @throws IOException if serialization fails
   */
  byte[] serialize(T object) throws IOException;

  /**
   * Deserializes a byte array back to an object.
   *
   * @param data the byte array to deserialize
   * @return the deserialized object
   * @throws IOException if deserialization fails
   */
  T deserialize(byte[] data) throws IOException;

  /**
   * Calculates the serialized size of an object without actually serializing it. This method is
   * used for storage optimization and capacity planning.
   *
   * @param object the object to calculate size for
   * @return the size in bytes that the object would occupy when serialized
   */
  int getSerializedSize(T object);
}
