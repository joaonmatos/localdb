package com.localdb.wal;

import com.localdb.serialization.SerializationStrategy;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * File-based implementation of the WriteAheadLog interface that stores WAL entries in a sequential
 * binary format on disk. This implementation provides durability and crash recovery by persisting
 * all operations before they are applied.
 *
 * <p>The file format consists of:
 *
 * <ul>
 *   <li>4-byte length prefix for each entry
 *   <li>Serialized entry data containing sequence number, transaction ID, operation type,
 *       timestamp, and serialized key/value/old-value data
 * </ul>
 *
 * <p>This class is thread-safe for concurrent reads and writes, using synchronized methods to
 * ensure consistency of the sequential sequence numbers and file writes.
 *
 * @param <K> the type of keys stored in WAL entries
 * @param <V> the type of values stored in WAL entries
 */
public class FileWriteAheadLog<K, V> implements WriteAheadLog<K, V> {
  private final Path walPath;
  private final SerializationStrategy<K> keySerializer;
  private final SerializationStrategy<V> valueSerializer;
  private final AtomicLong sequenceCounter;
  private BufferedOutputStream outputStream;

  public FileWriteAheadLog(
      Path walPath,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer)
      throws IOException {
    this.walPath = walPath;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.sequenceCounter = new AtomicLong(0);

    if (Files.exists(walPath)) {
      this.sequenceCounter.set(getLastSequenceNumber());
    }

    this.outputStream = new BufferedOutputStream(new FileOutputStream(walPath.toFile(), true));
  }

  @Override
  public synchronized void append(WALEntry<K, V> entry) throws IOException {
    var serializedEntry = serializeEntry(entry);
    var lengthPrefix = ByteBuffer.allocate(4).putInt(serializedEntry.length).array();

    outputStream.write(lengthPrefix);
    outputStream.write(serializedEntry);
  }

  @Override
  public synchronized void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public List<WALEntry<K, V>> readAll() throws IOException {
    return readFromSequence(0);
  }

  @Override
  public List<WALEntry<K, V>> readFromSequence(long sequenceNumber) throws IOException {
    var entries = new ArrayList<WALEntry<K, V>>();

    if (!Files.exists(walPath)) {
      return entries;
    }

    try (var inputStream = new BufferedInputStream(new FileInputStream(walPath.toFile()))) {

      while (inputStream.available() > 0) {
        var lengthBytes = new byte[4];
        if (inputStream.read(lengthBytes) != 4) {
          break;
        }

        var entryLength = ByteBuffer.wrap(lengthBytes).getInt();
        var entryBytes = new byte[entryLength];
        if (inputStream.read(entryBytes) != entryLength) {
          break;
        }

        var entry = deserializeEntry(entryBytes);
        if (entry.getSequenceNumber() >= sequenceNumber) {
          entries.add(entry);
        }
      }
    }

    return entries;
  }

  @Override
  public synchronized void truncate(long beforeSequenceNumber) throws IOException {
    var remainingEntries = new ArrayList<WALEntry<K, V>>();

    if (Files.exists(walPath)) {
      try (var inputStream = new BufferedInputStream(new FileInputStream(walPath.toFile()))) {

        while (inputStream.available() > 0) {
          var lengthBytes = new byte[4];
          if (inputStream.read(lengthBytes) != 4) {
            break;
          }

          var entryLength = ByteBuffer.wrap(lengthBytes).getInt();
          var entryBytes = new byte[entryLength];
          if (inputStream.read(entryBytes) != entryLength) {
            break;
          }

          var entry = deserializeEntry(entryBytes);
          if (entry.getSequenceNumber() >= beforeSequenceNumber) {
            remainingEntries.add(entry);
          }
        }
      }
    }

    outputStream.close();
    Files.deleteIfExists(walPath);
    outputStream = new BufferedOutputStream(new FileOutputStream(walPath.toFile()));

    for (var entry : remainingEntries) {
      append(entry);
    }
    flush();
  }

  @Override
  public synchronized void close() throws IOException {
    if (outputStream != null) {
      outputStream.close();
    }
  }

  /**
   * Returns the next sequence number for a new WAL entry. Sequence numbers are monotonically
   * increasing and unique.
   *
   * @return the next sequence number
   */
  public long getNextSequenceNumber() {
    return sequenceCounter.incrementAndGet();
  }

  /**
   * Reads the WAL file to determine the highest sequence number. This is used during initialization
   * to set the sequence counter.
   *
   * @return the last sequence number found in the WAL file
   * @throws IOException if an error occurs reading the WAL file
   */
  private long getLastSequenceNumber() throws IOException {
    long lastSequence = 0;

    if (!Files.exists(walPath)) {
      return lastSequence;
    }

    try (var inputStream = new BufferedInputStream(new FileInputStream(walPath.toFile()))) {

      while (inputStream.available() > 0) {
        var lengthBytes = new byte[4];
        if (inputStream.read(lengthBytes) != 4) {
          break;
        }

        var entryLength = ByteBuffer.wrap(lengthBytes).getInt();
        var entryBytes = new byte[entryLength];
        if (inputStream.read(entryBytes) != entryLength) {
          break;
        }

        var entry = deserializeEntry(entryBytes);
        lastSequence = Math.max(lastSequence, entry.getSequenceNumber());
      }
    }

    return lastSequence;
  }

  /**
   * Serializes a WAL entry to binary format for storage. The format includes all entry fields with
   * proper length prefixes.
   *
   * @param entry the WAL entry to serialize
   * @return the serialized entry as a byte array
   * @throws IOException if serialization fails
   */
  private byte[] serializeEntry(WALEntry<K, V> entry) throws IOException {
    var buffer = ByteBuffer.allocate(1024); // Initial capacity

    buffer.putLong(entry.getSequenceNumber());
    buffer.putLong(entry.getTransactionId());
    buffer.putInt(entry.getOperation().ordinal());
    buffer.putLong(entry.getTimestamp().toEpochMilli());

    var keyBytes = entry.getKey() != null ? keySerializer.serialize(entry.getKey()) : new byte[0];
    buffer.putInt(keyBytes.length);
    if (keyBytes.length > 0) {
      buffer.put(keyBytes);
    }

    var valueBytes =
        entry.getValue() != null ? valueSerializer.serialize(entry.getValue()) : new byte[0];
    buffer.putInt(valueBytes.length);
    if (valueBytes.length > 0) {
      buffer.put(valueBytes);
    }

    var oldValueBytes =
        entry.getOldValue() != null ? valueSerializer.serialize(entry.getOldValue()) : new byte[0];
    buffer.putInt(oldValueBytes.length);
    if (oldValueBytes.length > 0) {
      buffer.put(oldValueBytes);
    }

    buffer.flip();
    var result = new byte[buffer.remaining()];
    buffer.get(result);
    return result;
  }

  /**
   * Deserializes a WAL entry from binary format. This reverses the serialization process to
   * recreate the entry object.
   *
   * @param data the serialized entry data
   * @return the deserialized WAL entry
   * @throws IOException if deserialization fails
   */
  private WALEntry<K, V> deserializeEntry(byte[] data) throws IOException {
    var buffer = ByteBuffer.wrap(data);

    var sequenceNumber = buffer.getLong();
    var transactionId = buffer.getLong();
    var operation = WALEntry.OperationType.values()[buffer.getInt()];
    var timestampMillis = buffer.getLong();

    var keyLength = buffer.getInt();
    K key = null;
    if (keyLength > 0) {
      var keyBytes = new byte[keyLength];
      buffer.get(keyBytes);
      key = keySerializer.deserialize(keyBytes);
    }

    var valueLength = buffer.getInt();
    V value = null;
    if (valueLength > 0) {
      var valueBytes = new byte[valueLength];
      buffer.get(valueBytes);
      value = valueSerializer.deserialize(valueBytes);
    }

    var oldValueLength = buffer.getInt();
    V oldValue = null;
    if (oldValueLength > 0) {
      var oldValueBytes = new byte[oldValueLength];
      buffer.get(oldValueBytes);
      oldValue = valueSerializer.deserialize(oldValueBytes);
    }

    return new WALEntry<>(sequenceNumber, transactionId, operation, key, value, oldValue);
  }
}
