package com.localdb;

import com.localdb.transaction.Transaction;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * A high-performance, embedded key-value database that provides ACID transactions, range queries,
 * and persistent storage using a B+ tree data structure with write-ahead logging for durability.
 *
 * <p>This database supports:
 *
 * <ul>
 *   <li>Generic key-value storage with custom serialization strategies
 *   <li>ACID transactions with isolation and rollback support
 *   <li>Range queries for ordered key traversal
 *   <li>Thread-safe concurrent operations
 *   <li>Crash recovery through write-ahead logging
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * LocalDatabase<String, String> db = LocalDatabaseImpl.create(
 *     Paths.get("/data/mydb"),
 *     PrimitiveSerializationStrategy.STRING,
 *     PrimitiveSerializationStrategy.STRING);
 *
 * // Basic operations
 * db.put("key1", "value1");
 * Optional<String> value = db.get("key1");
 *
 * // Transactional operations
 * Transaction<String, String> tx = db.beginTransaction();
 * try {
 *     db.put("key2", "value2", tx);
 *     db.commitTransaction(tx);
 * } catch (Exception e) {
 *     db.rollbackTransaction(tx);
 * }
 *
 * db.close();
 * }</pre>
 *
 * @param <K> the type of keys stored in this database
 * @param <V> the type of values stored in this database
 */
public interface LocalDatabase<K, V> extends AutoCloseable {
  /**
   * Retrieves the value associated with the specified key.
   *
   * @param key the key whose associated value is to be returned
   * @return an {@code Optional} containing the value if found, or empty if not found
   * @throws IOException if an I/O error occurs during the operation
   */
  Optional<V> get(K key) throws IOException;

  /**
   * Retrieves the value associated with the specified key within the context of a transaction. This
   * method will return uncommitted values from the transaction if they exist, otherwise it falls
   * back to the committed state.
   *
   * @param key the key whose associated value is to be returned
   * @param transaction the transaction context, or null to read committed data
   * @return an {@code Optional} containing the value if found, or empty if not found
   * @throws IOException if an I/O error occurs during the operation
   */
  Optional<V> get(K key, Transaction<K, V> transaction) throws IOException;

  /**
   * Associates the specified value with the specified key in this database. This operation is
   * atomic and durable.
   *
   * @param key the key with which the specified value is to be associated
   * @param value the value to be associated with the specified key
   * @throws IOException if an I/O error occurs during the operation
   */
  void put(K key, V value) throws IOException;

  /**
   * Associates the specified value with the specified key within a transaction. The change will not
   * be visible outside the transaction until it is committed.
   *
   * @param key the key with which the specified value is to be associated
   * @param value the value to be associated with the specified key
   * @param transaction the transaction context
   * @throws IOException if an I/O error occurs during the operation
   * @throws IllegalArgumentException if the transaction is null or not active
   */
  void put(K key, V value, Transaction<K, V> transaction) throws IOException;

  /**
   * Removes the mapping for the specified key from this database if it exists. This operation is
   * atomic and durable.
   *
   * @param key the key whose mapping is to be removed
   * @return true if the key was present and removed, false if the key was not present
   * @throws IOException if an I/O error occurs during the operation
   */
  boolean delete(K key) throws IOException;

  /**
   * Removes the mapping for the specified key within a transaction. The change will not be visible
   * outside the transaction until it is committed.
   *
   * @param key the key whose mapping is to be removed
   * @param transaction the transaction context
   * @return true if the key was present and removed, false if the key was not present
   * @throws IOException if an I/O error occurs during the operation
   * @throws IllegalArgumentException if the transaction is null or not active
   */
  boolean delete(K key, Transaction<K, V> transaction) throws IOException;

  /**
   * Returns a list of values for all keys in the specified range (inclusive). The keys must
   * implement {@code Comparable} or a custom comparator must be provided.
   *
   * @param startKey the start of the range (inclusive)
   * @param endKey the end of the range (inclusive)
   * @return a list of values in the specified range, in key order
   * @throws IOException if an I/O error occurs during the operation
   */
  List<V> rangeQuery(K startKey, K endKey) throws IOException;

  /**
   * Returns a list of values for all keys in the specified range within a transaction context. This
   * includes uncommitted changes from the transaction.
   *
   * @param startKey the start of the range (inclusive)
   * @param endKey the end of the range (inclusive)
   * @param transaction the transaction context, or null to read committed data
   * @return a list of values in the specified range, in key order
   * @throws IOException if an I/O error occurs during the operation
   */
  List<V> rangeQuery(K startKey, K endKey, Transaction<K, V> transaction) throws IOException;

  /**
   * Begins a new transaction and returns a transaction handle. All operations within this
   * transaction are isolated from other transactions until the transaction is committed.
   *
   * @return a new transaction handle
   * @throws IOException if an I/O error occurs during transaction creation
   */
  Transaction<K, V> beginTransaction() throws IOException;

  /**
   * Commits the specified transaction, making all changes within the transaction visible to other
   * operations and persistent to storage.
   *
   * @param transaction the transaction to commit
   * @throws IOException if an I/O error occurs during the commit operation
   * @throws IllegalStateException if the transaction is not active
   */
  void commitTransaction(Transaction<K, V> transaction) throws IOException;

  /**
   * Rolls back the specified transaction, discarding all changes made within the transaction
   * without making them persistent.
   *
   * @param transaction the transaction to roll back
   * @throws IOException if an I/O error occurs during the rollback operation
   * @throws IllegalStateException if the transaction is not active
   */
  void rollbackTransaction(Transaction<K, V> transaction) throws IOException;

  /**
   * Returns true if this database contains a mapping for the specified key.
   *
   * @param key the key whose presence is to be tested
   * @return true if this database contains a mapping for the specified key
   * @throws IOException if an I/O error occurs during the operation
   */
  boolean containsKey(K key) throws IOException;

  /**
   * Returns true if this database contains a mapping for the specified key within the context of a
   * transaction.
   *
   * @param key the key whose presence is to be tested
   * @param transaction the transaction context, or null to check committed data
   * @return true if this database contains a mapping for the specified key
   * @throws IOException if an I/O error occurs during the operation
   */
  boolean containsKey(K key, Transaction<K, V> transaction) throws IOException;

  /**
   * Returns the number of key-value mappings in this database.
   *
   * @return the number of key-value mappings in this database
   * @throws IOException if an I/O error occurs during the operation
   */
  int size() throws IOException;

  /**
   * Returns true if this database contains no key-value mappings.
   *
   * @return true if this database contains no key-value mappings
   * @throws IOException if an I/O error occurs during the operation
   */
  boolean isEmpty() throws IOException;

  /**
   * Atomically compares the current value for the specified key with an expected value and, if they
   * are equal, updates the key to a new value. This operation is useful for implementing optimistic
   * concurrency control. The comparison is performed both at operation time and at commit time to
   * detect concurrent modifications.
   *
   * @param key the key to update
   * @param expectedValue the expected current value (null if expecting the key to not exist)
   * @param newValue the new value to set
   * @return true if the value was updated, false if the current value didn't match the expected
   *     value either at operation time or at commit time
   * @throws IOException if an I/O error occurs during the operation
   */
  boolean compareAndSet(K key, V expectedValue, V newValue) throws IOException;

  /**
   * Atomically compares the current value for the specified key with an expected value and, if they
   * are equal, updates the key to a new value within a transaction context. The comparison is
   * performed both at operation time and at commit time to detect concurrent modifications.
   *
   * @param key the key to update
   * @param expectedValue the expected current value (null if expecting the key to not exist)
   * @param newValue the new value to set
   * @param transaction the transaction context
   * @return true if the value was updated, false if the current value didn't match the expected
   *     value either at operation time or at commit time
   * @throws IOException if an I/O error occurs during the operation
   * @throws IllegalArgumentException if the transaction is null or not active
   */
  boolean compareAndSet(K key, V expectedValue, V newValue, Transaction<K, V> transaction)
      throws IOException;

  /**
   * Forces any buffered output to be written to persistent storage. This ensures that all previous
   * operations are durably stored.
   *
   * @throws IOException if an I/O error occurs during the flush operation
   */
  void flush() throws IOException;

  /**
   * Closes this database and releases all associated resources. Any active transactions will be
   * rolled back. After calling this method, no further operations should be performed on this
   * database.
   *
   * @throws IOException if an I/O error occurs during the close operation
   */
  @Override
  void close() throws IOException;
}
