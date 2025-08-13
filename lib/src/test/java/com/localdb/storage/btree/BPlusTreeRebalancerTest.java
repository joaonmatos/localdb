package com.localdb.storage.btree;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.localdb.serialization.primitives.PrimitiveSerializationStrategy;
import com.localdb.storage.Comparator;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BPlusTreeRebalancerTest {

  @TempDir Path tempDir;

  private Path dbFile;
  private BPlusTreeRebalancer<Integer, String> rebalancer;

  @BeforeEach
  void setUp() {
    dbFile = tempDir.resolve("test_tree.db");
    rebalancer =
        new BPlusTreeRebalancer<>(
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            4,
            1000);
  }

  @Test
  void testRebalanceEmptyTree() throws IOException {
    // Create empty tree
    try (var tree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            50)) {
      // Tree is empty
    }

    // Rebalancing empty tree should work without issues
    assertDoesNotThrow(() -> rebalancer.rebalance(dbFile));

    // Verify tree is still empty and functional
    try (var tree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            50)) {
      assertEquals(Optional.empty(), tree.search(1));
    }
  }

  @Test
  void testRebalanceWithData() throws IOException {
    // Create tree with some data
    try (var tree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            50)) {

      // Insert data in a pattern that might create imbalance
      for (int i = 0; i < 100; i++) {
        tree.insert(i, "value" + i);
      }

      // Delete some data to create potential imbalance
      for (int i = 10; i < 90; i += 2) {
        tree.delete(i);
      }
    }

    // Rebalance the tree
    assertDoesNotThrow(() -> rebalancer.rebalance(dbFile));

    // Verify all remaining data is still accessible
    try (var tree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            50)) {

      // Check that non-deleted values are still there
      for (int i = 0; i < 10; i++) {
        assertEquals(Optional.of("value" + i), tree.search(i));
      }

      for (int i = 90; i < 100; i++) {
        assertEquals(Optional.of("value" + i), tree.search(i));
      }

      // Check odd numbers between 10-90 are still there
      for (int i = 11; i < 90; i += 2) {
        assertEquals(Optional.of("value" + i), tree.search(i));
      }

      // Check that deleted values are gone
      for (int i = 10; i < 90; i += 2) {
        assertEquals(Optional.empty(), tree.search(i));
      }
    }
  }

  @Test
  void testRebalancePreservesOrder() throws IOException {
    // Create tree with data
    try (var tree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            50)) {

      // Insert data in reverse order to test ordering preservation
      for (int i = 50; i >= 0; i--) {
        tree.insert(i, "value" + i);
      }
    }

    // Rebalance
    rebalancer.rebalance(dbFile);

    // Verify order is preserved by checking range queries work correctly
    try (var tree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            50)) {

      // Get all pairs and verify they're in sorted order
      var pairs = tree.getAllPairs();
      assertEquals(51, pairs.size()); // 0 to 50 inclusive

      for (int i = 0; i < pairs.size(); i++) {
        assertEquals(Integer.valueOf(i), pairs.get(i).getKey());
        assertEquals("value" + i, pairs.get(i).getValue());
      }
    }
  }

  @Test
  void testRebalanceNonExistentFile() {
    Path nonExistentFile = tempDir.resolve("does_not_exist.db");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> rebalancer.rebalance(nonExistentFile));

    assertTrue(exception.getMessage().contains("Tree file does not exist"));
  }

  @Test
  void testGetTreeStats() throws IOException {
    // Create tree with some data
    try (var tree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            50)) {

      for (int i = 0; i < 20; i++) {
        tree.insert(i, "value" + i);
      }
    }

    // Get stats
    var stats = rebalancer.getTreeStats(dbFile);
    assertNotNull(stats);
    assertTrue(stats.getTotalKeys() > 0);
    assertTrue(stats.getTotalNodes() > 0);
    assertNotNull(stats.toString());
  }

  @Test
  void testRebalanceWithLargeDataset() throws IOException {
    // Create tree with larger dataset
    try (var tree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            1000)) {

      // Insert 500 values
      for (int i = 0; i < 500; i++) {
        tree.insert(i, "value" + i);
      }

      // Delete every 3rd value to create imbalance
      for (int i = 0; i < 500; i += 3) {
        tree.delete(i);
      }
    }

    // Rebalance
    long startTime = System.currentTimeMillis();
    rebalancer.rebalance(dbFile);
    long endTime = System.currentTimeMillis();

    System.out.println("Rebalancing 500 entries took " + (endTime - startTime) + " ms");

    // Verify correctness
    try (var tree =
        new PagedBPlusTree<>(
            dbFile,
            4,
            PrimitiveSerializationStrategy.INTEGER,
            PrimitiveSerializationStrategy.STRING,
            Comparator.INTEGER,
            1000)) {

      // Check remaining values
      for (int i = 0; i < 500; i++) {
        if (i % 3 == 0) {
          assertEquals(Optional.empty(), tree.search(i), "Key " + i + " should be deleted");
        } else {
          assertEquals(Optional.of("value" + i), tree.search(i), "Key " + i + " should exist");
        }
      }
    }
  }
}
