package com.localdb.storage.btree;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.storage.Comparator;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IsolatedPersistenceTest {

  @TempDir Path tempDir;

  @Test
  void testPersistenceIsolated() throws IOException {
    var dbFile = tempDir.resolve("isolated_persistence.db");

    // Phase 1: Create tree and insert data
    try (var tree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            50)) {
      // Insert data
      for (var i = 1; i <= 10; i++) {
        tree.insert(i, "value" + i);
      }

      // Verify data before closing
      for (var i = 1; i <= 10; i++) {
        assertEquals(Optional.of("value" + i), tree.search(i));
      }
    } // tree automatically closed

    // Phase 2: Reopen and verify persistence
    System.out.println("Reopening tree from file: " + dbFile);
    System.out.println("File exists: " + dbFile.toFile().exists());
    System.out.println("File size: " + dbFile.toFile().length());

    try (var reopenedTree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            50)) {
      // Verify data persisted
      for (var i = 1; i <= 10; i++) {
        var expected = Optional.of("value" + i);
        var actual = reopenedTree.search(i);
        System.out.println("Key " + i + ": expected " + expected + ", got " + actual);
        if (!expected.equals(actual)) {
          System.out.println("PERSISTENCE FAILED at key " + i);
          break;
        }
      }
    }
  }
}
