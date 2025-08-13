package com.localdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.storage.Comparator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocalDatabaseImplTest {

  private Path tempDataPath;
  private Path tempWalPath;
  private LocalDatabase<String, String> database;

  @BeforeEach
  void setUp() throws IOException {
    tempDataPath = Files.createTempFile("test-db", ".data");
    tempWalPath = Files.createTempFile("test-wal", ".log");

    database =
        LocalDatabaseImpl.create(
            tempDataPath,
            tempWalPath,
            4,
            PrimitiveSerializationStrategy.STRING,
            PrimitiveSerializationStrategy.STRING,
            Comparator.STRING);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (database != null) {
      database.close();
    }
    Files.deleteIfExists(tempDataPath);
    Files.deleteIfExists(tempWalPath);
  }

  @Test
  void testEmptyDatabase() throws IOException {
    assertTrue(database.isEmpty());
    assertEquals(0, database.size());
    assertEquals(Optional.empty(), database.get("key"));
    assertFalse(database.containsKey("key"));
  }

  @Test
  void testBasicOperations() throws IOException {
    database.put("key1", "value1");
    database.put("key2", "value2");

    assertFalse(database.isEmpty());
    assertEquals(2, database.size());
    assertEquals(Optional.of("value1"), database.get("key1"));
    assertEquals(Optional.of("value2"), database.get("key2"));
    assertTrue(database.containsKey("key1"));
    assertTrue(database.containsKey("key2"));
    assertFalse(database.containsKey("key3"));
  }

  @Test
  void testUpdate() throws IOException {
    database.put("key1", "value1");
    database.put("key1", "newValue1");

    assertEquals(1, database.size());
    assertEquals(Optional.of("newValue1"), database.get("key1"));
  }

  @Test
  void testDelete() throws IOException {
    database.put("key1", "value1");
    database.put("key2", "value2");

    assertTrue(database.delete("key1"));
    assertFalse(database.delete("key3"));

    assertEquals(1, database.size());
    assertEquals(Optional.empty(), database.get("key1"));
    assertEquals(Optional.of("value2"), database.get("key2"));
  }

  @Test
  void testRangeQuery() throws IOException {
    database.put("apple", "fruit1");
    database.put("banana", "fruit2");
    database.put("cherry", "fruit3");
    database.put("date", "fruit4");

    var result = database.rangeQuery("banana", "date");
    assertEquals(3, result.size());
  }

  @Test
  void testTransactions() throws IOException {
    var tx1 = database.beginTransaction();
    database.put("key1", "value1", tx1);
    database.put("key2", "value2", tx1);

    assertEquals(Optional.empty(), database.get("key1"));
    assertEquals(Optional.empty(), database.get("key2"));

    assertEquals(Optional.of("value1"), database.get("key1", tx1));
    assertEquals(Optional.of("value2"), database.get("key2", tx1));

    database.commitTransaction(tx1);

    assertEquals(Optional.of("value1"), database.get("key1"));
    assertEquals(Optional.of("value2"), database.get("key2"));
    assertEquals(2, database.size());
  }

  @Test
  void testTransactionRollback() throws IOException {
    database.put("key1", "originalValue");

    var tx = database.beginTransaction();
    database.put("key1", "newValue", tx);
    database.put("key2", "value2", tx);

    assertEquals(Optional.of("newValue"), database.get("key1", tx));
    assertEquals(Optional.of("value2"), database.get("key2", tx));

    database.rollbackTransaction(tx);

    assertEquals(Optional.of("originalValue"), database.get("key1"));
    assertEquals(Optional.empty(), database.get("key2"));
    assertEquals(1, database.size());
  }

  @Test
  void testTransactionDelete() throws IOException {
    database.put("key1", "value1");

    var tx = database.beginTransaction();
    assertTrue(database.delete("key1", tx));

    assertEquals(Optional.of("value1"), database.get("key1"));
    assertEquals(Optional.empty(), database.get("key1", tx));

    database.commitTransaction(tx);

    assertEquals(Optional.empty(), database.get("key1"));
    assertTrue(database.isEmpty());
  }

  @Test
  void testConcurrentOperations() throws IOException {
    for (int i = 0; i < 100; i++) {
      database.put("key" + i, "value" + i);
    }

    assertEquals(100, database.size());

    for (int i = 0; i < 100; i += 2) {
      database.delete("key" + i);
    }

    assertEquals(50, database.size());

    for (int i = 0; i < 100; i++) {
      if (i % 2 == 0) {
        assertEquals(Optional.empty(), database.get("key" + i));
      } else {
        assertEquals(Optional.of("value" + i), database.get("key" + i));
      }
    }
  }
}
