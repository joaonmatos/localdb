package com.localdb.transaction;

import com.localdb.wal.WALEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a database transaction that provides ACID properties for database operations. Each
 * transaction maintains its own list of operations and can be in one of three states: ACTIVE,
 * COMMITTED, or ABORTED.
 *
 * <p>Transactions provide isolation by maintaining a list of operations that are not visible to
 * other transactions until the transaction is committed. All operations within a transaction are
 * atomic - they either all succeed or all fail.
 *
 * <p>This class is thread-safe and can be used concurrently, though individual transactions should
 * typically be used by a single thread.
 *
 * @param <K> the type of keys in the transaction operations
 * @param <V> the type of values in the transaction operations
 */
public class Transaction<K, V> {
  private static final AtomicLong TRANSACTION_ID_GENERATOR = new AtomicLong(0);

  private final long transactionId;
  private final List<WALEntry<K, V>> operations;
  private TransactionState state;
  private final Object lock = new Object();

  public Transaction() {
    this.transactionId = TRANSACTION_ID_GENERATOR.incrementAndGet();
    this.operations = new ArrayList<>();
    this.state = TransactionState.ACTIVE;
  }

  /**
   * Returns the unique identifier for this transaction.
   *
   * @return the transaction ID
   */
  public long getTransactionId() {
    return transactionId;
  }

  /**
   * Returns the current state of this transaction.
   *
   * @return the current transaction state
   */
  public TransactionState getState() {
    synchronized (lock) {
      return state;
    }
  }

  /**
   * Sets the state of this transaction.
   *
   * @param state the new transaction state
   */
  public void setState(TransactionState state) {
    synchronized (lock) {
      this.state = state;
    }
  }

  /**
   * Adds an operation to this transaction's operation list.
   *
   * @param entry the WAL entry representing the operation
   * @throws IllegalStateException if the transaction is not active
   */
  public void addOperation(WALEntry<K, V> entry) {
    synchronized (lock) {
      if (state != TransactionState.ACTIVE) {
        throw new IllegalStateException("Cannot add operations to non-active transaction");
      }
      operations.add(entry);
    }
  }

  /**
   * Returns a copy of all operations performed within this transaction.
   *
   * @return a new list containing all transaction operations
   */
  public List<WALEntry<K, V>> getOperations() {
    synchronized (lock) {
      return new ArrayList<>(operations);
    }
  }

  /**
   * Returns true if this transaction is currently active and accepting operations.
   *
   * @return true if the transaction is active
   */
  public boolean isActive() {
    return getState() == TransactionState.ACTIVE;
  }

  /**
   * Returns true if this transaction has been successfully committed.
   *
   * @return true if the transaction is committed
   */
  public boolean isCommitted() {
    return getState() == TransactionState.COMMITTED;
  }

  /**
   * Returns true if this transaction has been aborted or rolled back.
   *
   * @return true if the transaction is aborted
   */
  public boolean isAborted() {
    return getState() == TransactionState.ABORTED;
  }
}
