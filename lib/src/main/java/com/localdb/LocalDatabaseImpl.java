package com.localdb;

import com.localdb.serialization.SerializationStrategy;
import com.localdb.storage.Comparator;
import com.localdb.storage.btree.BPlusTree;
import com.localdb.transaction.Transaction;
import com.localdb.transaction.TransactionManager;
import com.localdb.wal.FileWriteAheadLog;
import com.localdb.wal.WALEntry;
import com.localdb.wal.WriteAheadLog;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link LocalDatabase} that provides a persistent, ACID-compliant
 * key-value store using a B+ tree for indexing and write-ahead logging for durability and crash
 * recovery.
 *
 * <p>This implementation features:
 *
 * <ul>
 *   <li>Thread-safe concurrent access with read-write locking
 *   <li>B+ tree storage for efficient range queries and indexing
 *   <li>Write-ahead logging for crash recovery and durability
 *   <li>ACID transactions with isolation and rollback support
 *   <li>Configurable B+ tree order for performance tuning
 *   <li>Custom serialization strategies for keys and values
 * </ul>
 *
 * <p>The database automatically recovers from crashes by replaying the write-ahead log on startup.
 * All operations are logged before being applied to the B+ tree, ensuring consistency in case of
 * system failures.
 *
 * @param <K> the type of keys stored in this database
 * @param <V> the type of values stored in this database
 */
public class LocalDatabaseImpl<K, V> implements LocalDatabase<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(LocalDatabaseImpl.class);
  private static final int DEFAULT_BTREE_ORDER = 128;

  private final BPlusTree<K, V> btree;
  private final WriteAheadLog<K, V> wal;
  private final TransactionManager<K, V> transactionManager;
  private final ReadWriteLock databaseLock;
  private volatile boolean closed = false;

  private LocalDatabaseImpl(
      BPlusTree<K, V> btree, WriteAheadLog<K, V> wal, TransactionManager<K, V> transactionManager) {
    this.btree = btree;
    this.wal = wal;
    this.transactionManager = transactionManager;
    this.databaseLock = new ReentrantReadWriteLock();
  }

  /**
   * Creates a new LocalDatabase instance with default settings in the specified directory. This
   * method uses the default B+ tree order and natural key comparison.
   *
   * @param <K> the key type, must implement {@code Comparable}
   * @param <V> the value type
   * @param directoryPath the directory where database files will be stored
   * @param keySerializer the serialization strategy for keys
   * @param valueSerializer the serialization strategy for values
   * @return a new LocalDatabase instance
   * @throws IOException if an I/O error occurs during database creation
   */
  public static <K extends Comparable<K>, V> LocalDatabase<K, V> create(
      Path directoryPath,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer)
      throws IOException {
    return create(
        directoryPath.resolve("data.db"),
        directoryPath.resolve("wal.log"),
        DEFAULT_BTREE_ORDER,
        keySerializer,
        valueSerializer,
        Comparator.natural());
  }

  /**
   * Creates a new LocalDatabase instance with a custom comparator in the specified directory. This
   * method uses the default B+ tree order.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @param directoryPath the directory where database files will be stored
   * @param keySerializer the serialization strategy for keys
   * @param valueSerializer the serialization strategy for values
   * @param comparator the comparator for ordering keys
   * @return a new LocalDatabase instance
   * @throws IOException if an I/O error occurs during database creation
   */
  public static <K, V> LocalDatabase<K, V> create(
      Path directoryPath,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator)
      throws IOException {
    return create(
        directoryPath.resolve("data.db"),
        directoryPath.resolve("wal.log"),
        DEFAULT_BTREE_ORDER,
        keySerializer,
        valueSerializer,
        comparator);
  }

  /**
   * Creates a new LocalDatabase instance with full customization options. This is the most flexible
   * factory method allowing specification of all parameters.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @param dataPath the path for the main database file
   * @param walPath the path for the write-ahead log file
   * @param btreeOrder the order (maximum number of children) for B+ tree nodes
   * @param keySerializer the serialization strategy for keys
   * @param valueSerializer the serialization strategy for values
   * @param comparator the comparator for ordering keys
   * @return a new LocalDatabase instance
   * @throws IOException if an I/O error occurs during database creation
   */
  public static <K, V> LocalDatabase<K, V> create(
      Path dataPath,
      Path walPath,
      int btreeOrder,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator)
      throws IOException {

    var wal = new FileWriteAheadLog<>(walPath, keySerializer, valueSerializer);
    var transactionManager = new TransactionManager<>(wal);
    var btree = new BPlusTree<>(btreeOrder, keySerializer, valueSerializer, comparator);

    var database = new LocalDatabaseImpl<>(btree, wal, transactionManager);

    database.recover();
    return database;
  }

  @Override
  public Optional<V> get(K key) throws IOException {
    checkClosed();
    databaseLock.readLock().lock();
    try {
      return btree.search(key);
    } finally {
      databaseLock.readLock().unlock();
    }
  }

  @Override
  public Optional<V> get(K key, Transaction<K, V> transaction) throws IOException {
    checkClosed();
    if (transaction == null || !transaction.isActive()) {
      return get(key);
    }

    databaseLock.readLock().lock();
    try {
      if (hasUncommittedOperation(key, transaction)) {
        return getUncommittedValue(key, transaction);
      }
      return btree.search(key);
    } finally {
      databaseLock.readLock().unlock();
    }
  }

  @Override
  public void put(K key, V value) throws IOException {
    Transaction<K, V> transaction = beginTransaction();
    try {
      put(key, value, transaction);
      commitTransaction(transaction);
    } catch (Exception e) {
      rollbackTransaction(transaction);
      throw e;
    }
  }

  @Override
  public void put(K key, V value, Transaction<K, V> transaction) throws IOException {
    checkClosed();
    if (transaction == null || !transaction.isActive()) {
      throw new IllegalArgumentException("Transaction must be active");
    }

    databaseLock.writeLock().lock();
    try {
      var oldValue = btree.search(key);

      var entry =
          new WALEntry<>(
              getNextSequenceNumber(),
              transaction.getTransactionId(),
              oldValue.isPresent() ? WALEntry.OperationType.UPDATE : WALEntry.OperationType.INSERT,
              key,
              value,
              oldValue.orElse(null));

      transactionManager.addOperationToTransaction(transaction, entry);

    } finally {
      databaseLock.writeLock().unlock();
    }
  }

  @Override
  public boolean delete(K key) throws IOException {
    Transaction<K, V> transaction = beginTransaction();
    try {
      boolean result = delete(key, transaction);
      commitTransaction(transaction);
      return result;
    } catch (Exception e) {
      rollbackTransaction(transaction);
      throw e;
    }
  }

  @Override
  public boolean delete(K key, Transaction<K, V> transaction) throws IOException {
    checkClosed();
    if (transaction == null || !transaction.isActive()) {
      throw new IllegalArgumentException("Transaction must be active");
    }

    databaseLock.writeLock().lock();
    try {
      var oldValue = btree.search(key);
      if (oldValue.isEmpty()) {
        return false;
      }

      var entry =
          new WALEntry<>(
              getNextSequenceNumber(),
              transaction.getTransactionId(),
              WALEntry.OperationType.DELETE,
              key,
              null,
              oldValue.get());

      transactionManager.addOperationToTransaction(transaction, entry);
      return true;

    } finally {
      databaseLock.writeLock().unlock();
    }
  }

  @Override
  public List<V> rangeQuery(K startKey, K endKey) throws IOException {
    checkClosed();
    databaseLock.readLock().lock();
    try {
      return btree.rangeQuery(startKey, endKey);
    } finally {
      databaseLock.readLock().unlock();
    }
  }

  @Override
  public List<V> rangeQuery(K startKey, K endKey, Transaction<K, V> transaction)
      throws IOException {
    checkClosed();
    if (transaction == null || !transaction.isActive()) {
      return rangeQuery(startKey, endKey);
    }

    databaseLock.readLock().lock();
    try {
      return btree.rangeQuery(startKey, endKey);
    } finally {
      databaseLock.readLock().unlock();
    }
  }

  @Override
  public Transaction<K, V> beginTransaction() throws IOException {
    checkClosed();
    return transactionManager.beginTransaction();
  }

  @Override
  public void commitTransaction(Transaction<K, V> transaction) throws IOException {
    checkClosed();
    databaseLock.writeLock().lock();
    try {
      applyTransactionOperations(transaction);
      transactionManager.commitTransaction(transaction);
      wal.flush();
    } finally {
      databaseLock.writeLock().unlock();
    }
  }

  @Override
  public void rollbackTransaction(Transaction<K, V> transaction) throws IOException {
    checkClosed();
    transactionManager.rollbackTransaction(transaction);
    wal.flush();
  }

  @Override
  public boolean containsKey(K key) throws IOException {
    return get(key).isPresent();
  }

  @Override
  public boolean containsKey(K key, Transaction<K, V> transaction) throws IOException {
    return get(key, transaction).isPresent();
  }

  @Override
  public int size() throws IOException {
    checkClosed();
    databaseLock.readLock().lock();
    try {
      return btree.size();
    } finally {
      databaseLock.readLock().unlock();
    }
  }

  @Override
  public boolean isEmpty() throws IOException {
    checkClosed();
    databaseLock.readLock().lock();
    try {
      return btree.isEmpty();
    } finally {
      databaseLock.readLock().unlock();
    }
  }

  @Override
  public void flush() throws IOException {
    checkClosed();
    wal.flush();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    logger.info("Shutting down LocalDatabase...");

    databaseLock.writeLock().lock();
    try {
      transactionManager.shutdown();
      wal.close();
      closed = true;
      logger.info("LocalDatabase shutdown complete");
    } finally {
      databaseLock.writeLock().unlock();
    }
  }

  /**
   * Recovers the database state from the write-ahead log on startup. This method replays all
   * committed transactions from the WAL to restore the B+ tree to its last consistent state.
   *
   * @throws IOException if an I/O error occurs during recovery
   */
  private void recover() throws IOException {
    logger.info("Starting database recovery...");

    transactionManager.recoverTransactions();

    var committedEntries = wal.readAll();
    for (var entry : committedEntries) {
      if (entry.getOperation() == WALEntry.OperationType.INSERT
          || entry.getOperation() == WALEntry.OperationType.UPDATE) {
        btree.insert(entry.getKey(), entry.getValue());
      } else if (entry.getOperation() == WALEntry.OperationType.DELETE) {
        btree.delete(entry.getKey());
      }
    }

    logger.info("Database recovery completed");
  }

  /**
   * Applies all operations from a committed transaction to the B+ tree. This method processes
   * INSERT, UPDATE, and DELETE operations in sequence.
   *
   * @param transaction the transaction whose operations should be applied
   * @throws IOException if an I/O error occurs during operation application
   */
  private void applyTransactionOperations(Transaction<K, V> transaction) throws IOException {
    for (var entry : transaction.getOperations()) {
      switch (entry.getOperation()) {
        case INSERT, UPDATE -> btree.insert(entry.getKey(), entry.getValue());
        case DELETE -> btree.delete(entry.getKey());
        case TRANSACTION_BEGIN, TRANSACTION_COMMIT, TRANSACTION_ROLLBACK -> {
          // These operations are not data operations and should not be applied here
        }
      }
    }
  }

  /**
   * Checks if a transaction has any uncommitted operations for the specified key. This method
   * searches the transaction's operation list in reverse chronological order.
   *
   * @param key the key to check for uncommitted operations
   * @param transaction the transaction to search
   * @return true if the transaction has uncommitted operations for the key
   */
  private boolean hasUncommittedOperation(K key, Transaction<K, V> transaction) {
    var operations = transaction.getOperations();

    for (int i = operations.size() - 1; i >= 0; i--) {
      var entry = operations.get(i);
      if (entry.getKey().equals(key)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Retrieves the most recent uncommitted value for a key within a transaction. If the most recent
   * operation was a DELETE, returns empty.
   *
   * @param key the key to look up
   * @param transaction the transaction to search
   * @return the uncommitted value if found, or empty if deleted or not found
   */
  private Optional<V> getUncommittedValue(K key, Transaction<K, V> transaction) {
    var operations = transaction.getOperations();

    for (int i = operations.size() - 1; i >= 0; i--) {
      var entry = operations.get(i);
      if (entry.getKey().equals(key)) {
        if (entry.getOperation() == WALEntry.OperationType.DELETE) {
          return Optional.empty();
        } else {
          return Optional.of(entry.getValue());
        }
      }
    }

    return Optional.empty();
  }

  private long getNextSequenceNumber() {
    return ((FileWriteAheadLog<K, V>) wal).getNextSequenceNumber();
  }

  /**
   * Checks if the database has been closed and throws an exception if so. This method should be
   * called at the beginning of all public operations.
   *
   * @throws IllegalStateException if the database has been closed
   */
  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("Database is closed");
    }
  }
}
