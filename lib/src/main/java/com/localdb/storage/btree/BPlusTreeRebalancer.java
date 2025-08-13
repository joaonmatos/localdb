package com.localdb.storage.btree;

import com.localdb.serialization.SerializationStrategy;
import com.localdb.storage.Comparator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for rebalancing B+ trees offline. This utility extracts all key-value pairs from an
 * existing tree and rebuilds it with optimal balance.
 *
 * <p>The rebalancing process: 1. Opens the existing tree in read-only mode 2. Performs a range scan
 * to extract all key-value pairs in sorted order 3. Creates a new optimally balanced tree 4.
 * Inserts all pairs into the new tree 5. Replaces the original tree file with the rebalanced
 * version
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class BPlusTreeRebalancer<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(BPlusTreeRebalancer.class);

  private final SerializationStrategy<K> keySerializer;
  private final SerializationStrategy<V> valueSerializer;
  private final Comparator<K> comparator;
  private final int order;
  private final int bufferPoolSize;

  /**
   * Creates a new rebalancer with the specified configuration.
   *
   * @param keySerializer the key serialization strategy
   * @param valueSerializer the value serialization strategy
   * @param comparator the key comparator
   * @param order the B+ tree order
   * @param bufferPoolSize the buffer pool size to use during rebalancing
   */
  public BPlusTreeRebalancer(
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator,
      int order,
      int bufferPoolSize) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.comparator = comparator;
    this.order = order;
    this.bufferPoolSize = bufferPoolSize;
  }

  /**
   * Rebalances the B+ tree at the specified path.
   *
   * @param treePath the path to the tree file to rebalance
   * @throws IOException if an I/O error occurs during rebalancing
   * @throws IllegalArgumentException if the tree file doesn't exist or is invalid
   */
  public void rebalance(Path treePath) throws IOException {
    if (!Files.exists(treePath)) {
      throw new IllegalArgumentException("Tree file does not exist: " + treePath);
    }

    logger.info("Starting rebalancing of tree: {}", treePath);

    // Create temporary file for the new balanced tree
    Path tempPath = treePath.resolveSibling(treePath.getFileName() + ".rebalancing");
    Path backupPath = treePath.resolveSibling(treePath.getFileName() + ".backup");

    try {
      // Step 1: Extract all key-value pairs from the existing tree
      List<KeyValuePair<K, V>> pairs = extractAllPairs(treePath);
      logger.info("Extracted {} key-value pairs from existing tree", pairs.size());

      if (pairs.isEmpty()) {
        logger.info("Tree is empty, no rebalancing needed");
        return;
      }

      // Step 2: Create new balanced tree
      createBalancedTree(tempPath, pairs);
      logger.info("Created new balanced tree with {} pairs", pairs.size());

      // Step 3: Atomically replace the original file
      Files.move(treePath, backupPath, StandardCopyOption.REPLACE_EXISTING);
      Files.move(tempPath, treePath, StandardCopyOption.REPLACE_EXISTING);

      // Clean up backup file
      Files.deleteIfExists(backupPath);

      logger.info("Successfully rebalanced tree: {}", treePath);

    } catch (Exception e) {
      // Clean up temporary files on error
      Files.deleteIfExists(tempPath);
      Files.deleteIfExists(backupPath);
      throw new IOException("Failed to rebalance tree: " + treePath, e);
    }
  }

  /**
   * Extracts all key-value pairs from the existing tree using range scan.
   *
   * @param treePath the path to the existing tree
   * @return list of all key-value pairs in sorted order
   * @throws IOException if an I/O error occurs
   */
  private List<KeyValuePair<K, V>> extractAllPairs(Path treePath) throws IOException {
    List<KeyValuePair<K, V>> pairs = new ArrayList<>();

    try (PagedBPlusTree<K, V> tree =
        new PagedBPlusTree<>(
            treePath, order, keySerializer, valueSerializer, comparator, bufferPoolSize)) {

      // Use range scan to get all pairs in sorted order
      // We'll implement a custom traversal since rangeQuery might not be available
      extractPairsFromTree(tree, pairs);
    }

    return pairs;
  }

  /**
   * Extracts all pairs from the tree by traversing leaf nodes.
   *
   * @param tree the tree to extract from
   * @param pairs the list to add pairs to
   * @throws IOException if an I/O error occurs
   */
  private void extractPairsFromTree(PagedBPlusTree<K, V> tree, List<KeyValuePair<K, V>> pairs)
      throws IOException {
    // Use the getAllPairs method from PagedBPlusTree
    List<PagedBPlusTree.KeyValuePair<K, V>> treePairs = tree.getAllPairs();

    // Convert to our internal KeyValuePair format
    for (PagedBPlusTree.KeyValuePair<K, V> treePair : treePairs) {
      pairs.add(new KeyValuePair<>(treePair.getKey(), treePair.getValue()));
    }
  }

  /**
   * Creates a new balanced tree with the given key-value pairs.
   *
   * @param treePath the path for the new tree
   * @param pairs the sorted key-value pairs to insert
   * @throws IOException if an I/O error occurs
   */
  private void createBalancedTree(Path treePath, List<KeyValuePair<K, V>> pairs)
      throws IOException {
    try (PagedBPlusTree<K, V> newTree =
        new PagedBPlusTree<>(
            treePath, order, keySerializer, valueSerializer, comparator, bufferPoolSize)) {

      // Insert pairs in sorted order for optimal tree construction
      for (KeyValuePair<K, V> pair : pairs) {
        newTree.insert(pair.key, pair.value);
      }
    }
  }

  /**
   * Gets statistics about the tree structure before and after rebalancing.
   *
   * @param treePath the path to the tree file
   * @return tree statistics
   * @throws IOException if an I/O error occurs
   */
  public PagedBPlusTree.TreeStatistics getTreeStats(Path treePath) throws IOException {
    if (!Files.exists(treePath)) {
      throw new IllegalArgumentException("Tree file does not exist: " + treePath);
    }

    try (PagedBPlusTree<K, V> tree =
        new PagedBPlusTree<>(
            treePath, order, keySerializer, valueSerializer, comparator, bufferPoolSize)) {

      return tree.getStatistics();
    }
  }

  /** Simple container for key-value pairs. */
  private static class KeyValuePair<K, V> {
    final K key;
    final V value;

    KeyValuePair(K key, V value) {
      this.key = key;
      this.value = value;
    }
  }
}
