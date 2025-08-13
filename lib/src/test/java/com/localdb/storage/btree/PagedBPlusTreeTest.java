package com.localdb.storage.btree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.storage.Comparator;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PagedBPlusTreeTest {

  @TempDir Path tempDir;

  private Path dbFile;
  private PagedBPlusTree<Integer, String> tree;

  @BeforeEach
  void setUp() throws IOException {
    // Use unique file for each test to avoid interference
    dbFile = tempDir.resolve("test" + System.nanoTime() + ".db");
    tree =
        new PagedBPlusTree<>(
            dbFile,
            4, // Order 4 for small trees that split easily
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            50 // Small buffer pool for testing eviction
            );
  }

  @AfterEach
  void tearDown() throws IOException {
    if (tree != null) {
      try {
        tree.close();
      } catch (Exception e) {
        // Ignore close errors in cleanup
      } finally {
        tree = null; // Ensure clean state
      }
    }
    // Clean up file if it exists
    if (dbFile != null && dbFile.toFile().exists()) {
      try {
        dbFile.toFile().delete();
      } catch (Exception e) {
        // Ignore file deletion errors in cleanup
      }
    }
  }

  @Test
  void testEmptyTree() throws IOException {
    assertTrue(tree.isEmpty());
    assertEquals(Optional.empty(), tree.search(1));
  }

  @Test
  void testSingleInsertAndSearch() throws IOException {
    tree.insert(1, "one");

    assertFalse(tree.isEmpty());
    assertEquals(Optional.of("one"), tree.search(1));
    assertEquals(Optional.empty(), tree.search(2));
  }

  @Test
  void testMultipleInsertsAndSearches() throws IOException {
    tree.insert(3, "three");
    tree.insert(1, "one");
    tree.insert(4, "four");
    tree.insert(2, "two");

    assertFalse(tree.isEmpty());
    assertEquals(Optional.of("one"), tree.search(1));
    assertEquals(Optional.of("two"), tree.search(2));
    assertEquals(Optional.of("three"), tree.search(3));
    assertEquals(Optional.of("four"), tree.search(4));
    assertEquals(Optional.empty(), tree.search(5));
  }

  @Test
  void testUpdateValue() throws IOException {
    tree.insert(1, "one");
    tree.insert(1, "ONE");

    assertEquals(Optional.of("ONE"), tree.search(1));
  }

  @Test
  void testDelete() throws IOException {
    tree.insert(1, "one");
    tree.insert(2, "two");
    tree.insert(3, "three");

    assertTrue(tree.delete(2));
    assertEquals(Optional.empty(), tree.search(2));
    assertEquals(Optional.of("one"), tree.search(1));
    assertEquals(Optional.of("three"), tree.search(3));

    assertFalse(tree.delete(2)); // Already deleted
    assertFalse(tree.delete(5)); // Never existed
  }

  @Test
  void testRangeQuery() throws IOException {
    for (var i = 1; i <= 10; i++) {
      tree.insert(i, "value" + i);
    }

    var result = tree.rangeQueryList(3, 7);
    assertEquals(5, result.size());
    assertEquals(List.of("value3", "value4", "value5", "value6", "value7"), result);

    var allResults = tree.rangeQueryList(1, 10);
    assertEquals(10, allResults.size());

    var emptyResult = tree.rangeQueryList(15, 20);
    assertTrue(emptyResult.isEmpty());
  }

  @Test
  void testLargeDataset() throws IOException {
    // Use a larger buffer pool for this test to handle many pages
    var largeDbFile = tempDir.resolve("large_test" + System.nanoTime() + ".db");
    try (var largeTree =
        new PagedBPlusTree<>(
            largeDbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            1000 // Larger buffer pool for large dataset
            )) {
      var count = 1000;

      // Insert many values to test tree splitting and disk I/O
      for (var i = 0; i < count; i++) {
        largeTree.insert(i, "value" + i);
      }

      // Verify all values can be found
      for (var i = 0; i < count; i++) {
        assertEquals(Optional.of("value" + i), largeTree.search(i));
      }

      // Delete every other value
      for (var i = 0; i < count; i += 2) {
        assertTrue(largeTree.delete(i));
      }

      // Verify deletions and remaining values
      for (var i = 0; i < count; i++) {
        if (i % 2 == 0) {
          assertEquals(Optional.empty(), largeTree.search(i));
        } else {
          assertEquals(Optional.of("value" + i), largeTree.search(i));
        }
      }
    }
  }

  @Test
  void testPersistence() throws IOException {
    // Insert data
    for (var i = 1; i <= 10; i++) {
      tree.insert(i, "value" + i);
    }

    // Close the original tree
    tree.close();
    tree = null; // Clear reference to avoid double-close in @AfterEach

    // Reopen tree with a new instance
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
        assertEquals(Optional.of("value" + i), reopenedTree.search(i));
      }
    }
  }

  @Test
  void testBufferPoolEviction() throws IOException {
    // Insert enough data to cause buffer pool eviction
    var count = 60; // Slightly more than buffer pool size (50)

    for (var i = 0; i < count; i++) {
      tree.insert(i, "value" + i);
    }

    // Verify data is still accessible (should load from disk)
    for (var i = 0; i < count; i++) {
      assertEquals(Optional.of("value" + i), tree.search(i));
    }
  }

  @Test
  void testSync() throws IOException {
    tree.insert(1, "one");
    tree.insert(2, "two");

    // Should not throw exception
    tree.sync();

    // Data should still be accessible
    assertEquals(Optional.of("one"), tree.search(1));
    assertEquals(Optional.of("two"), tree.search(2));
  }

  @Test
  void testBufferPoolStats() throws IOException {
    var stats = tree.getBufferPoolStats();
    assertTrue(stats.contains("Buffer Pool"));
    assertTrue(stats.contains("/50"));
  }

  @Test
  void testRandomInsertionOrder() throws IOException {
    // Test with a randomized insertion order
    var values = new int[] {5, 2, 8, 1, 7, 3, 9, 4, 6, 10};

    for (var value : values) {
      tree.insert(value, "value" + value);
    }

    // Verify all values are accessible in sorted order
    for (var i = 1; i <= 10; i++) {
      assertEquals(Optional.of("value" + i), tree.search(i));
    }

    // Test range query returns sorted results
    var rangeResult = tree.rangeQueryList(3, 7);
    assertEquals(List.of("value3", "value4", "value5", "value6", "value7"), rangeResult);
  }

  @Test
  void testSplitBehavior() throws IOException {
    // Insert values that will cause multiple splits
    for (var i = 1; i <= 20; i++) {
      tree.insert(i, "value" + i);
    }

    // Tree should handle splits correctly and maintain order
    for (var i = 1; i <= 20; i++) {
      assertEquals(Optional.of("value" + i), tree.search(i));
    }

    // Range query should work across split boundaries
    var result = tree.rangeQueryList(1, 20);
    assertEquals(20, result.size());
    for (var i = 0; i < 20; i++) {
      assertEquals("value" + (i + 1), result.get(i));
    }
  }
}
