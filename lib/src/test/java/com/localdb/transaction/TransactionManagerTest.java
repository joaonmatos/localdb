package com.localdb.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.wal.FileWriteAheadLog;
import com.localdb.wal.WALEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TransactionManagerTest {

  private Path tempWalPath;
  private FileWriteAheadLog<String, String> wal;
  private TransactionManager<String, String> transactionManager;

  @BeforeEach
  void setUp() throws IOException {
    tempWalPath = Files.createTempFile("test-tx-wal", ".log");
    wal =
        new FileWriteAheadLog<>(
            tempWalPath,
            PrimitiveSerializationStrategy.STRING,
            PrimitiveSerializationStrategy.STRING);
    transactionManager = new TransactionManager<>(wal);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (wal != null) {
      wal.close();
    }
    Files.deleteIfExists(tempWalPath);
  }

  @Test
  void testBeginTransaction() throws IOException {
    Transaction<String, String> transaction = transactionManager.beginTransaction();

    assertNotNull(transaction);
    assertTrue(transaction.isActive());
    assertFalse(transaction.isCommitted());
    assertFalse(transaction.isAborted());
  }

  @Test
  void testCommitTransaction() throws IOException {
    Transaction<String, String> transaction = transactionManager.beginTransaction();
    transactionManager.commitTransaction(transaction);

    assertTrue(transaction.isCommitted());
    assertFalse(transaction.isActive());
    assertFalse(transaction.isAborted());
  }

  @Test
  void testRollbackTransaction() throws IOException {
    Transaction<String, String> transaction = transactionManager.beginTransaction();
    transactionManager.rollbackTransaction(transaction);

    assertTrue(transaction.isAborted());
    assertFalse(transaction.isActive());
    assertFalse(transaction.isCommitted());
  }

  @Test
  void testAddOperationToTransaction() throws IOException {
    Transaction<String, String> transaction = transactionManager.beginTransaction();

    WALEntry<String, String> entry =
        new WALEntry<>(
            wal.getNextSequenceNumber(),
            transaction.getTransactionId(),
            WALEntry.OperationType.INSERT,
            "key",
            "value",
            null);

    transactionManager.addOperationToTransaction(transaction, entry);

    assertEquals(1, transaction.getOperations().size());
    assertEquals(entry, transaction.getOperations().get(0));
  }

  @Test
  void testRecoverTransactions() throws IOException {
    transactionManager.recoverTransactions();
    // If no exceptions, recovery succeeded
  }

  @Test
  void testIsTransactionActive() throws IOException {
    Transaction<String, String> transaction = transactionManager.beginTransaction();

    assertTrue(transactionManager.isTransactionActive(transaction.getTransactionId()));

    transactionManager.commitTransaction(transaction);

    assertFalse(transactionManager.isTransactionActive(transaction.getTransactionId()));
  }
}
