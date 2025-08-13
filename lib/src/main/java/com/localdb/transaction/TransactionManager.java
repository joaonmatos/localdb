package com.localdb.transaction;

import com.localdb.wal.WALEntry;
import com.localdb.wal.WriteAheadLog;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages database transactions, providing ACID properties and coordinating transaction lifecycle
 * with the write-ahead log for durability and recovery.
 *
 * <p>This manager is responsible for:
 *
 * <ul>
 *   <li>Creating and tracking active transactions
 *   <li>Coordinating transaction commits and rollbacks with the WAL
 *   <li>Providing transaction recovery during database startup
 *   <li>Managing concurrent access to transaction state
 * </ul>
 *
 * <p>All transaction operations are logged to the write-ahead log before being applied, ensuring
 * durability and enabling crash recovery.
 *
 * @param <K> the type of keys managed by this transaction manager
 * @param <V> the type of values managed by this transaction manager
 */
public class TransactionManager<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);

  private final Map<Long, Transaction<K, V>> activeTransactions;
  private final WriteAheadLog<K, V> wal;
  private final ReadWriteLock globalLock;

  public TransactionManager(WriteAheadLog<K, V> wal) {
    this.activeTransactions = new ConcurrentHashMap<>();
    this.wal = wal;
    this.globalLock = new ReentrantReadWriteLock();
  }

  /**
   * Creates and starts a new transaction, logging the begin event to the WAL.
   *
   * @return a new active transaction
   * @throws IOException if an error occurs writing to the WAL
   */
  public Transaction<K, V> beginTransaction() throws IOException {
    var transaction = new Transaction<K, V>();

    activeTransactions.put(transaction.getTransactionId(), transaction);

    var beginEntry =
        new WALEntry<K, V>(
            getNextSequenceNumber(),
            transaction.getTransactionId(),
            WALEntry.OperationType.TRANSACTION_BEGIN,
            null,
            null,
            null);

    wal.append(beginEntry);
    wal.flush();

    logger.debug("Started transaction {}", transaction.getTransactionId());
    return transaction;
  }

  /**
   * Commits the specified transaction, making all its operations permanent. This operation is
   * atomic and durable.
   *
   * @param transaction the transaction to commit
   * @throws IOException if an error occurs during commit
   * @throws IllegalStateException if the transaction is not active
   */
  public void commitTransaction(Transaction<K, V> transaction) throws IOException {
    globalLock.writeLock().lock();
    try {
      if (!transaction.isActive()) {
        throw new IllegalStateException("Transaction is not active");
      }

      transaction.setState(TransactionState.COMMITTED);

      var commitEntry =
          new WALEntry<K, V>(
              getNextSequenceNumber(),
              transaction.getTransactionId(),
              WALEntry.OperationType.TRANSACTION_COMMIT,
              null,
              null,
              null);

      wal.append(commitEntry);
      wal.flush();

      activeTransactions.remove(transaction.getTransactionId());

      logger.debug("Committed transaction {}", transaction.getTransactionId());
    } finally {
      globalLock.writeLock().unlock();
    }
  }

  /**
   * Rolls back the specified transaction, discarding all its operations.
   *
   * @param transaction the transaction to roll back
   * @throws IOException if an error occurs during rollback
   * @throws IllegalStateException if the transaction is not active
   */
  public void rollbackTransaction(Transaction<K, V> transaction) throws IOException {
    globalLock.writeLock().lock();
    try {
      if (!transaction.isActive()) {
        throw new IllegalStateException("Transaction is not active");
      }

      transaction.setState(TransactionState.ABORTED);

      var rollbackEntry =
          new WALEntry<K, V>(
              getNextSequenceNumber(),
              transaction.getTransactionId(),
              WALEntry.OperationType.TRANSACTION_ROLLBACK,
              null,
              null,
              null);

      wal.append(rollbackEntry);
      wal.flush();

      activeTransactions.remove(transaction.getTransactionId());

      logger.debug("Rolled back transaction {}", transaction.getTransactionId());
    } finally {
      globalLock.writeLock().unlock();
    }
  }

  /**
   * Adds an operation to the specified transaction and logs it to the WAL.
   *
   * @param transaction the transaction to add the operation to
   * @param entry the WAL entry representing the operation
   * @throws IOException if an error occurs writing to the WAL
   * @throws IllegalStateException if the transaction is not active
   */
  public void addOperationToTransaction(Transaction<K, V> transaction, WALEntry<K, V> entry)
      throws IOException {
    globalLock.readLock().lock();
    try {
      if (!transaction.isActive()) {
        throw new IllegalStateException("Transaction is not active");
      }

      transaction.addOperation(entry);
      wal.append(entry);
    } finally {
      globalLock.readLock().unlock();
    }
  }

  /**
   * Retrieves an active transaction by its ID.
   *
   * @param transactionId the ID of the transaction to retrieve
   * @return the transaction if found and active, null otherwise
   */
  public Transaction<K, V> getTransaction(long transactionId) {
    return activeTransactions.get(transactionId);
  }

  /**
   * Checks if a transaction with the specified ID is currently active.
   *
   * @param transactionId the ID of the transaction to check
   * @return true if the transaction exists and is active
   */
  public boolean isTransactionActive(long transactionId) {
    var transaction = activeTransactions.get(transactionId);
    return transaction != null && transaction.isActive();
  }

  /**
   * Recovers transaction state from the write-ahead log during database startup. This method
   * replays all transaction operations from the WAL and rolls back any transactions that were not
   * properly committed.
   *
   * @throws IOException if an error occurs reading from the WAL
   */
  public void recoverTransactions() throws IOException {
    logger.info("Starting transaction recovery...");

    for (var entry : wal.readAll()) {
      switch (entry.getOperation()) {
        case TRANSACTION_BEGIN -> {
          var transaction = new Transaction<K, V>();
          activeTransactions.put(entry.getTransactionId(), transaction);
          logger.debug("Recovered transaction begin: {}", entry.getTransactionId());
        }
        case TRANSACTION_COMMIT -> {
          var committedTx = activeTransactions.remove(entry.getTransactionId());
          if (committedTx != null) {
            committedTx.setState(TransactionState.COMMITTED);
          }
          logger.debug("Recovered transaction commit: {}", entry.getTransactionId());
        }
        case TRANSACTION_ROLLBACK -> {
          var rolledBackTx = activeTransactions.remove(entry.getTransactionId());
          if (rolledBackTx != null) {
            rolledBackTx.setState(TransactionState.ABORTED);
          }
          logger.debug("Recovered transaction rollback: {}", entry.getTransactionId());
        }
        case INSERT, UPDATE, DELETE -> {
          var operationTx = activeTransactions.get(entry.getTransactionId());
          if (operationTx != null) {
            operationTx.addOperation(entry);
          }
        }
      }
    }

    for (var orphanedTx : activeTransactions.values()) {
      logger.warn("Rolling back orphaned transaction: {}", orphanedTx.getTransactionId());
      rollbackTransaction(orphanedTx);
    }

    logger.info("Transaction recovery completed");
  }

  private long getNextSequenceNumber() {
    return ((com.localdb.wal.FileWriteAheadLog<K, V>) wal).getNextSequenceNumber();
  }

  /**
   * Shuts down the transaction manager, rolling back all active transactions. This method should be
   * called during database shutdown.
   *
   * @throws IOException if an error occurs during shutdown
   */
  public void shutdown() throws IOException {
    logger.info("Shutting down transaction manager...");

    for (var transaction : activeTransactions.values()) {
      logger.warn(
          "Rolling back active transaction during shutdown: {}", transaction.getTransactionId());
      rollbackTransaction(transaction);
    }

    activeTransactions.clear();
  }
}
