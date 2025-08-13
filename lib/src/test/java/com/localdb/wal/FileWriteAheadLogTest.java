package com.localdb.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FileWriteAheadLogTest {

  private Path tempWalPath;
  private FileWriteAheadLog<String, String> wal;

  @BeforeEach
  void setUp() throws IOException {
    tempWalPath = Files.createTempFile("test-wal", ".log");
    wal =
        new FileWriteAheadLog<>(
            tempWalPath,
            PrimitiveSerializationStrategy.STRING,
            PrimitiveSerializationStrategy.STRING);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (wal != null) {
      wal.close();
    }
    Files.deleteIfExists(tempWalPath);
  }

  @Test
  void testEmptyLog() throws IOException {
    var entries = wal.readAll();
    assertTrue(entries.isEmpty());
  }

  @Test
  void testAppendAndRead() throws IOException {
    var entry1 = new WALEntry<>(1L, 100L, WALEntry.OperationType.INSERT, "key1", "value1", null);
    var entry2 =
        new WALEntry<>(2L, 100L, WALEntry.OperationType.UPDATE, "key1", "newValue1", "value1");

    wal.append(entry1);
    wal.append(entry2);
    wal.flush();

    var entries = wal.readAll();
    assertEquals(2, entries.size());

    assertEquals(1L, entries.get(0).getSequenceNumber());
    assertEquals(100L, entries.get(0).getTransactionId());
    assertEquals(WALEntry.OperationType.INSERT, entries.get(0).getOperation());
    assertEquals("key1", entries.get(0).getKey());
    assertEquals("value1", entries.get(0).getValue());

    assertEquals(2L, entries.get(1).getSequenceNumber());
    assertEquals(WALEntry.OperationType.UPDATE, entries.get(1).getOperation());
    assertEquals("newValue1", entries.get(1).getValue());
    assertEquals("value1", entries.get(1).getOldValue());
  }

  @Test
  void testReadFromSequence() throws IOException {
    for (long i = 1; i <= 10; i++) {
      var entry =
          new WALEntry<>(i, 100L, WALEntry.OperationType.INSERT, "key" + i, "value" + i, null);
      wal.append(entry);
    }
    wal.flush();

    List<WALEntry<String, String>> entries = wal.readFromSequence(5L);
    assertEquals(6, entries.size());
    assertEquals(5L, entries.get(0).getSequenceNumber());
    assertEquals(10L, entries.get(5).getSequenceNumber());
  }

  @Test
  void testTruncate() throws IOException {
    for (long i = 1; i <= 10; i++) {
      var entry =
          new WALEntry<>(i, 100L, WALEntry.OperationType.INSERT, "key" + i, "value" + i, null);
      wal.append(entry);
    }
    wal.flush();

    wal.truncate(5L);

    var entries = wal.readAll();
    assertEquals(6, entries.size());
    assertEquals(5L, entries.get(0).getSequenceNumber());
    assertEquals(10L, entries.get(5).getSequenceNumber());
  }

  @Test
  void testTransactionEntries() throws IOException {
    WALEntry<String, String> beginEntry =
        new WALEntry<>(1L, 100L, WALEntry.OperationType.TRANSACTION_BEGIN, null, null, null);
    WALEntry<String, String> insertEntry =
        new WALEntry<>(2L, 100L, WALEntry.OperationType.INSERT, "key1", "value1", null);
    WALEntry<String, String> commitEntry =
        new WALEntry<>(3L, 100L, WALEntry.OperationType.TRANSACTION_COMMIT, null, null, null);

    wal.append(beginEntry);
    wal.append(insertEntry);
    wal.append(commitEntry);
    wal.flush();

    var entries = wal.readAll();
    assertEquals(3, entries.size());
    assertEquals(WALEntry.OperationType.TRANSACTION_BEGIN, entries.get(0).getOperation());
    assertEquals(WALEntry.OperationType.INSERT, entries.get(1).getOperation());
    assertEquals(WALEntry.OperationType.TRANSACTION_COMMIT, entries.get(2).getOperation());
  }
}
