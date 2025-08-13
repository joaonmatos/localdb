package com.localdb.wal;

import com.localdb.serialization.SerializationStrategy;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Write-Ahead Log interface that provides durability and crash recovery for database operations.
 * The WAL ensures that all changes are logged to persistent storage before being applied to the
 * main data structures.
 *
 * <p>The write-ahead log maintains a sequential record of all database operations, including
 * transaction boundaries and data modifications. This allows the database to recover to a
 * consistent state after a crash by replaying the logged operations.
 *
 * @param <K> the type of keys stored in WAL entries
 * @param <V> the type of values stored in WAL entries
 */
public interface WriteAheadLog<K, V> {
  /**
   * Appends a new entry to the write-ahead log.
   *
   * @param entry the WAL entry to append
   * @throws IOException if an I/O error occurs during the append operation
   */
  void append(WALEntry<K, V> entry) throws IOException;

  /**
   * Forces all buffered WAL entries to be written to persistent storage.
   *
   * @throws IOException if an I/O error occurs during the flush operation
   */
  void flush() throws IOException;

  /**
   * Reads all entries from the write-ahead log.
   *
   * @return a list of all WAL entries in chronological order
   * @throws IOException if an I/O error occurs during the read operation
   */
  List<WALEntry<K, V>> readAll() throws IOException;

  /**
   * Reads all entries from the log starting from the specified sequence number.
   *
   * @param sequenceNumber the sequence number to start reading from (inclusive)
   * @return a list of WAL entries from the specified sequence onward
   * @throws IOException if an I/O error occurs during the read operation
   */
  List<WALEntry<K, V>> readFromSequence(long sequenceNumber) throws IOException;

  /**
   * Truncates the log, removing all entries before the specified sequence number. This is typically
   * used for log compaction after applying entries to the main data structure.
   *
   * @param beforeSequenceNumber the sequence number before which all entries will be removed
   * @throws IOException if an I/O error occurs during the truncation operation
   */
  void truncate(long beforeSequenceNumber) throws IOException;

  /**
   * Closes the write-ahead log and releases all associated resources.
   *
   * @throws IOException if an I/O error occurs during the close operation
   */
  void close() throws IOException;

  /**
   * Creates a new file-based write-ahead log instance.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @param walPath the path where the WAL file will be stored
   * @param keySerializer the serialization strategy for keys
   * @param valueSerializer the serialization strategy for values
   * @return a new WriteAheadLog instance
   * @throws IOException if an I/O error occurs during WAL creation
   */
  static <K, V> WriteAheadLog<K, V> create(
      Path walPath,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer)
      throws IOException {
    return new FileWriteAheadLog<>(walPath, keySerializer, valueSerializer);
  }
}
