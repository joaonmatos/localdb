package com.localdb.wal;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents a single entry in the Write-Ahead Log, containing all information about a database
 * operation including its sequence number, transaction context, operation type, and associated
 * data.
 *
 * <p>WAL entries are immutable and contain a timestamp for auditing purposes. They support both
 * data operations (INSERT, UPDATE, DELETE) and transaction control operations (TRANSACTION_BEGIN,
 * TRANSACTION_COMMIT, TRANSACTION_ROLLBACK).
 *
 * @param <K> the type of keys in this WAL entry
 * @param <V> the type of values in this WAL entry
 */
public class WALEntry<K, V> {
  /** Enumeration of all possible operation types that can be logged in the WAL. */
  public enum OperationType {
    /** Insert operation - adds a new key-value pair */
    INSERT,
    /** Update operation - modifies an existing key-value pair */
    UPDATE,
    /** Delete operation - removes a key-value pair */
    DELETE,
    /** Compare-and-set operation - conditionally updates a key-value pair */
    COMPARE_AND_SET,
    /** Transaction begin marker */
    TRANSACTION_BEGIN,
    /** Transaction commit marker */
    TRANSACTION_COMMIT,
    /** Transaction rollback marker */
    TRANSACTION_ROLLBACK
  }

  private final long sequenceNumber;
  private final long transactionId;
  private final OperationType operation;
  private final K key;
  private final V value;
  private final V oldValue;
  private final V expectedValue;
  private final Instant timestamp;

  /**
   * Creates a new WAL entry with the specified parameters.
   *
   * @param sequenceNumber the unique sequence number for this entry
   * @param transactionId the ID of the transaction this entry belongs to
   * @param operation the type of operation being logged
   * @param key the key involved in the operation (may be null for transaction markers)
   * @param value the new value (may be null for deletes and transaction markers)
   * @param oldValue the previous value (may be null for inserts and transaction markers)
   */
  public WALEntry(
      long sequenceNumber,
      long transactionId,
      OperationType operation,
      K key,
      V value,
      V oldValue) {
    this(sequenceNumber, transactionId, operation, key, value, oldValue, null);
  }

  /**
   * Creates a new WAL entry with the specified parameters, including expected value for
   * compare-and-set operations.
   *
   * @param sequenceNumber the unique sequence number for this entry
   * @param transactionId the ID of the transaction this entry belongs to
   * @param operation the type of operation being logged
   * @param key the key involved in the operation (may be null for transaction markers)
   * @param value the new value (may be null for deletes and transaction markers)
   * @param oldValue the previous value (may be null for inserts and transaction markers)
   * @param expectedValue the expected value for compare-and-set operations (null for other
   *     operations)
   */
  public WALEntry(
      long sequenceNumber,
      long transactionId,
      OperationType operation,
      K key,
      V value,
      V oldValue,
      V expectedValue) {
    this.sequenceNumber = sequenceNumber;
    this.transactionId = transactionId;
    this.operation = operation;
    this.key = key;
    this.value = value;
    this.oldValue = oldValue;
    this.expectedValue = expectedValue;
    this.timestamp = Instant.now();
  }

  /**
   * Returns the sequence number of this WAL entry.
   *
   * @return the sequence number
   */
  public long getSequenceNumber() {
    return sequenceNumber;
  }

  /**
   * Returns the transaction ID associated with this entry.
   *
   * @return the transaction ID
   */
  public long getTransactionId() {
    return transactionId;
  }

  /**
   * Returns the operation type of this entry.
   *
   * @return the operation type
   */
  public OperationType getOperation() {
    return operation;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public V getOldValue() {
    return oldValue;
  }

  public V getExpectedValue() {
    return expectedValue;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WALEntry<?, ?> walEntry = (WALEntry<?, ?>) o;
    return sequenceNumber == walEntry.sequenceNumber
        && transactionId == walEntry.transactionId
        && operation == walEntry.operation
        && Objects.equals(key, walEntry.key)
        && Objects.equals(value, walEntry.value)
        && Objects.equals(oldValue, walEntry.oldValue)
        && Objects.equals(expectedValue, walEntry.expectedValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        sequenceNumber, transactionId, operation, key, value, oldValue, expectedValue);
  }

  @Override
  public String toString() {
    return "WALEntry{"
        + "sequenceNumber="
        + sequenceNumber
        + ", transactionId="
        + transactionId
        + ", operation="
        + operation
        + ", key="
        + key
        + ", value="
        + value
        + ", oldValue="
        + oldValue
        + ", expectedValue="
        + expectedValue
        + ", timestamp="
        + timestamp
        + '}';
  }
}
