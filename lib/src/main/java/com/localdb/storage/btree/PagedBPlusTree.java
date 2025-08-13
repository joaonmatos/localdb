package com.localdb.storage.btree;

import com.localdb.serialization.SerializationStrategy;
import com.localdb.storage.Comparator;
import com.localdb.storage.buffer.BufferPool;
import com.localdb.storage.page.DiskManager;
import com.localdb.storage.page.PageId;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * A disk-backed B+Tree implementation that uses pages for storage. This implementation only loads
 * required pages into memory and uses a buffer pool with LRU eviction for caching hot pages.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Disk-backed storage with page-based I/O
 *   <li>LRU buffer pool for caching frequently accessed pages
 *   <li>Lazy loading of tree nodes
 *   <li>Scalable to datasets larger than available RAM
 *   <li>Thread-safe operations
 * </ul>
 *
 * @param <K> the type of keys stored in the tree
 * @param <V> the type of values stored in the tree
 */
public final class PagedBPlusTree<K, V> implements AutoCloseable {

  private final int order;
  private final SerializationStrategy<K> keySerializer;
  private final SerializationStrategy<V> valueSerializer;
  private final Comparator<K> comparator;
  private final DiskManager diskManager;
  private final BufferPool bufferPool;

  private PageId rootPageId;

  /**
   * Creates a new paged B+Tree with the specified configuration.
   *
   * @param filePath the path to the database file
   * @param order the order of the B+Tree (max keys per node)
   * @param keySerializer serialization strategy for keys
   * @param valueSerializer serialization strategy for values
   * @param comparator comparator for keys
   * @param bufferPoolSize the size of the buffer pool
   * @throws IOException if the database file cannot be accessed
   */
  public PagedBPlusTree(
      Path filePath,
      int order,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator,
      int bufferPoolSize)
      throws IOException {
    this.order = order;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.comparator = comparator;
    this.diskManager = new DiskManager(filePath);
    this.bufferPool = new BufferPool(diskManager, bufferPoolSize);

    initializeTree();
  }

  /**
   * Creates a new paged B+Tree with default buffer pool size.
   *
   * @param filePath the path to the database file
   * @param order the order of the B+Tree
   * @param keySerializer serialization strategy for keys
   * @param valueSerializer serialization strategy for values
   * @param comparator comparator for keys
   * @throws IOException if the database file cannot be accessed
   */
  public PagedBPlusTree(
      Path filePath,
      int order,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator)
      throws IOException {
    this(filePath, order, keySerializer, valueSerializer, comparator, BufferPool.DEFAULT_POOL_SIZE);
  }

  /**
   * Searches for a value associated with the specified key.
   *
   * @param key the key to search for
   * @return an Optional containing the value if found, empty otherwise
   * @throws IOException if an I/O error occurs
   */
  public Optional<V> search(K key) throws IOException {
    if (rootPageId == null) {
      return Optional.empty(); // Empty tree
    }

    // Check if root page is initialized
    var page = bufferPool.fetchPage(rootPageId);
    try {
      var data = page.readData();
      if (data.length == 0 || data[0] == 0) {
        return Optional.empty(); // Empty tree - page exists but no node data written
      }
    } finally {
      bufferPool.unpinPage(rootPageId, false);
    }

    var root = loadRootNode();
    var result = root.search(key);
    return result.isFound() ? Optional.of(result.getValue()) : Optional.empty();
  }

  /**
   * Inserts or updates a key-value pair in the tree.
   *
   * @param key the key to insert
   * @param value the value to associate with the key
   * @throws IOException if an I/O error occurs
   */
  public void insert(K key, V value) throws IOException {
    var root = loadRootNode();
    var result = root.insert(key, value);

    // Save the root node if it was modified
    root.savePage();

    if (result.isSplitOccurred()) {
      // Create a new root
      var newRootPage = bufferPool.newPage();
      var newRoot =
          new PagedBPlusTreeInternalNode<>(
              newRootPage.getPageId(),
              bufferPool,
              keySerializer,
              valueSerializer,
              comparator,
              order);

      newRoot.getKeys().add(result.getPromotedKey());
      newRoot.addChildPageId(rootPageId);
      newRoot.addChildPageId(result.getNewNode().getPageId());

      // Save new root
      newRoot.savePage();

      // Update root page ID
      rootPageId = newRoot.getPageId();

      // Update metadata page with new root page ID
      updateRootPageId();

      // Save the new node from split
      result.getNewNode().savePage();

      // Unpin pages
      bufferPool.unpinPage(newRootPage.getPageId(), false);
    }
  }

  /**
   * Deletes the key-value pair associated with the specified key.
   *
   * @param key the key to delete
   * @return true if the key was found and deleted, false otherwise
   * @throws IOException if an I/O error occurs
   */
  public boolean delete(K key) throws IOException {
    var root = loadRootNode();
    var result = root.delete(key);

    // Save the root node if it was modified
    root.savePage();

    // Handle root underflow (tree height reduction)
    if (!root.isLeaf() && root.getKeys().isEmpty()) {
      var internalRoot = (PagedBPlusTreeInternalNode<K, V>) root;
      if (!internalRoot.getChildPageIds().isEmpty()) {
        rootPageId = internalRoot.getChildPageIds().get(0);
        // Update metadata page with new root page ID
        updateRootPageId();
      }
    }

    return result.isDeleted();
  }

  /**
   * Performs a range query, returning an iterator over values for keys in the specified range. The
   * iterator lazily loads pages from disk as needed, enabling efficient processing of large result
   * sets without loading all data into memory.
   *
   * @param startKey the start of the range (inclusive)
   * @param endKey the end of the range (inclusive)
   * @return an iterator over values in the specified range, in key order
   * @throws IOException if an I/O error occurs
   */
  public Iterator<V> rangeQuery(K startKey, K endKey) throws IOException {
    var startLeaf = findFirstLeaf(startKey);
    var startIndex = startLeaf.getKeys().size(); // Default to past end of leaf

    // Find the starting index within the leaf
    var keys = startLeaf.getKeys();
    for (var i = 0; i < keys.size(); i++) {
      if (comparator.compare(keys.get(i), startKey) >= 0) {
        startIndex = i;
        break;
      }
    }

    return new PagedBPlusTreeIterator<>(startLeaf, startIndex, endKey, comparator);
  }

  /**
   * Performs a range query, returning a list of all values for keys in the specified range. For
   * large result sets, consider using the iterator-based rangeQuery method instead.
   *
   * @param startKey the start of the range (inclusive)
   * @param endKey the end of the range (inclusive)
   * @return a list of values in the specified range, in key order
   * @throws IOException if an I/O error occurs
   */
  public List<V> rangeQueryList(K startKey, K endKey) throws IOException {
    var result = new ArrayList<V>();
    var iterator = rangeQuery(startKey, endKey);

    while (iterator.hasNext()) {
      result.add(iterator.next());
    }

    return result;
  }

  /**
   * Returns true if this tree contains no key-value pairs.
   *
   * @return true if the tree is empty
   * @throws IOException if an I/O error occurs
   */
  public boolean isEmpty() throws IOException {
    var root = loadRootNode();
    return root.getKeys().isEmpty();
  }

  /**
   * Forces all dirty pages to be written to disk.
   *
   * @throws IOException if an I/O error occurs
   */
  public void sync() throws IOException {
    bufferPool.flushAllPages();
    diskManager.sync();
  }

  /**
   * Gets all key-value pairs in the tree in sorted order. This method is useful for tree
   * rebalancing and backup operations.
   *
   * @return a list of all key-value pairs in sorted order
   * @throws IOException if an I/O error occurs
   */
  public List<KeyValuePair<K, V>> getAllPairs() throws IOException {
    List<KeyValuePair<K, V>> pairs = new ArrayList<>();

    if (rootPageId == null) {
      return pairs; // Empty tree
    }

    // Check if the root page actually contains valid node data
    var page = bufferPool.fetchPage(rootPageId);
    try {
      var data = page.readData();
      if (data.length == 0 || data[0] == 0) {
        return pairs; // Empty tree - page exists but no node data written
      }
      if (data[0] != PagedBPlusTreeNode.LEAF_NODE_TYPE
          && data[0] != PagedBPlusTreeNode.INTERNAL_NODE_TYPE) {
        return pairs; // Invalid node type
      }
    } finally {
      bufferPool.unpinPage(rootPageId, false);
    }

    // Find the leftmost leaf node
    PagedBPlusTreeNode<K, V> current = loadNode(rootPageId);
    while (current instanceof PagedBPlusTreeInternalNode) {
      PagedBPlusTreeInternalNode<K, V> internal = (PagedBPlusTreeInternalNode<K, V>) current;
      current = internal.loadChild(0); // Go to leftmost child
    }

    // Traverse all leaf nodes using the leaf chain
    while (current != null) {
      PagedBPlusTreeLeafNode<K, V> leaf = (PagedBPlusTreeLeafNode<K, V>) current;

      // Add all pairs from this leaf
      for (int i = 0; i < leaf.keys.size(); i++) {
        pairs.add(new KeyValuePair<>(leaf.keys.get(i), leaf.getValues().get(i)));
      }

      // Move to next leaf
      if (leaf.getNextLeafPageId() != null && leaf.getNextLeafPageId().isValid()) {
        current = loadNode(leaf.getNextLeafPageId());
      } else {
        current = null;
      }
    }

    return pairs;
  }

  /**
   * Gets statistics about the tree structure. This method traverses the entire tree to collect
   * detailed statistics.
   *
   * @return tree statistics
   * @throws IOException if an I/O error occurs
   */
  public TreeStatistics getStatistics() throws IOException {
    if (rootPageId == null) {
      return new TreeStatistics(0, 0, 0, 0, 0.0, 0);
    }

    TreeStatisticsCollector collector = new TreeStatisticsCollector();
    collectStatistics(loadNode(rootPageId), collector, 0);

    double avgFillRatio =
        collector.totalNodes > 0
            ? (double) collector.totalKeys / (collector.totalNodes * (order - 1))
            : 0.0;

    return new TreeStatistics(
        collector.totalNodes,
        collector.leafNodes,
        collector.internalNodes,
        collector.totalKeys,
        avgFillRatio,
        collector.maxDepth);
  }

  private void collectStatistics(
      PagedBPlusTreeNode<K, V> node, TreeStatisticsCollector collector, int depth)
      throws IOException {
    collector.totalNodes++;
    collector.maxDepth = Math.max(collector.maxDepth, depth);

    if (node instanceof PagedBPlusTreeLeafNode) {
      PagedBPlusTreeLeafNode<K, V> leaf = (PagedBPlusTreeLeafNode<K, V>) node;
      collector.leafNodes++;
      collector.totalKeys += leaf.keys.size();
    } else if (node instanceof PagedBPlusTreeInternalNode) {
      PagedBPlusTreeInternalNode<K, V> internal = (PagedBPlusTreeInternalNode<K, V>) node;
      collector.internalNodes++;
      collector.totalKeys += internal.keys.size();

      // Recursively collect statistics from children
      for (int i = 0; i < internal.getChildPageIds().size(); i++) {
        PagedBPlusTreeNode<K, V> child = internal.loadChild(i);
        collectStatistics(child, collector, depth + 1);
      }
    }
  }

  private static class TreeStatisticsCollector {
    int totalNodes = 0;
    int leafNodes = 0;
    int internalNodes = 0;
    int totalKeys = 0;
    int maxDepth = 0;
  }

  /** Statistics about tree structure. */
  public static class TreeStatistics {
    private final int totalNodes;
    private final int leafNodes;
    private final int internalNodes;
    private final int totalKeys;
    private final double averageFillRatio;
    private final int maxDepth;

    public TreeStatistics(
        int totalNodes,
        int leafNodes,
        int internalNodes,
        int totalKeys,
        double averageFillRatio,
        int maxDepth) {
      this.totalNodes = totalNodes;
      this.leafNodes = leafNodes;
      this.internalNodes = internalNodes;
      this.totalKeys = totalKeys;
      this.averageFillRatio = averageFillRatio;
      this.maxDepth = maxDepth;
    }

    public int getTotalNodes() {
      return totalNodes;
    }

    public int getLeafNodes() {
      return leafNodes;
    }

    public int getInternalNodes() {
      return internalNodes;
    }

    public int getTotalKeys() {
      return totalKeys;
    }

    public double getAverageFillRatio() {
      return averageFillRatio;
    }

    public int getMaxDepth() {
      return maxDepth;
    }

    @Override
    public String toString() {
      return String.format(
          "TreeStatistics{totalNodes=%d, leafNodes=%d, internalNodes=%d, totalKeys=%d, avgFillRatio=%.2f, maxDepth=%d}",
          totalNodes, leafNodes, internalNodes, totalKeys, averageFillRatio, maxDepth);
    }
  }

  /** Simple container for key-value pairs. */
  public static class KeyValuePair<K, V> {
    private final K key;
    private final V value;

    public KeyValuePair(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.format("KeyValuePair{key=%s, value=%s}", key, value);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      sync();
    } finally {
      try {
        bufferPool.close();
      } finally {
        diskManager.close();
      }
    }
  }

  /**
   * Gets statistics about the buffer pool usage.
   *
   * @return a string describing buffer pool statistics
   */
  public String getBufferPoolStats() {
    return String.format(
        "Buffer Pool: %d/%d pages", bufferPool.getSize(), bufferPool.getCapacity());
  }

  /**
   * Initializes the tree, creating a root node if necessary.
   *
   * @throws IOException if an I/O error occurs
   */
  private void initializeTree() throws IOException {
    // Check if the database file exists and has content
    if (diskManager.getFileSize() == 0) {
      // Create metadata page at page 0
      var metadataPage = bufferPool.newPage(); // This will be page 0
      var metadataPageId = metadataPage.getPageId();

      // Create initial root leaf node
      var rootPage = bufferPool.newPage(); // This will be page 1
      rootPageId = rootPage.getPageId();

      try {
        var rootLeaf =
            new PagedBPlusTreeLeafNode<>(
                rootPageId, bufferPool, keySerializer, valueSerializer, comparator, order);

        // Mark the new root leaf as dirty so it gets properly serialized
        rootLeaf.markDirty();
        rootLeaf.savePage();

        // Store root page ID in metadata page
        saveRootPageId(metadataPageId);
      } finally {
        bufferPool.unpinPage(rootPage.getPageId(), false);
        bufferPool.unpinPage(metadataPageId, false);
      }
    } else {
      // Load root page ID from metadata page
      rootPageId = loadRootPageId();
    }
  }

  /**
   * Loads the root node from disk.
   *
   * @return the root node
   * @throws IOException if an I/O error occurs
   */
  private PagedBPlusTreeNode<K, V> loadRootNode() throws IOException {
    return loadNode(rootPageId);
  }

  /**
   * Loads a node from the specified page ID.
   *
   * @param pageId the page ID to load
   * @return the loaded node
   * @throws IOException if an I/O error occurs
   */
  private PagedBPlusTreeNode<K, V> loadNode(PageId pageId) throws IOException {
    // Determine if node is leaf or internal by examining the page
    var page = bufferPool.fetchPage(pageId);
    PagedBPlusTreeNode<K, V> node;

    try {
      var data = page.readData();

      if (data.length > 0 && data[0] == PagedBPlusTreeNode.LEAF_NODE_TYPE) {
        node =
            new PagedBPlusTreeLeafNode<>(
                pageId, bufferPool, keySerializer, valueSerializer, comparator, order);
        node.deserialize(data);
      } else if (data.length > 0 && data[0] == PagedBPlusTreeNode.INTERNAL_NODE_TYPE) {
        node =
            new PagedBPlusTreeInternalNode<>(
                pageId, bufferPool, keySerializer, valueSerializer, comparator, order);
        node.deserialize(data);
      } else {
        throw new IOException("Invalid node type or empty page: " + pageId);
      }
    } finally {
      bufferPool.unpinPage(pageId, false);
    }

    return node;
  }

  private PagedBPlusTreeNode<K, V> loadRootNodeOld() throws IOException {
    // Determine if root is leaf or internal by examining the page
    var page = bufferPool.fetchPage(rootPageId);
    PagedBPlusTreeNode<K, V> root;

    try {
      var data = page.readData();

      if (data.length > 0 && data[0] == PagedBPlusTreeNode.LEAF_NODE_TYPE) {
        root =
            new PagedBPlusTreeLeafNode<>(
                rootPageId, bufferPool, keySerializer, valueSerializer, comparator, order);
        root.deserialize(data);
      } else if (data.length > 0 && data[0] == PagedBPlusTreeNode.INTERNAL_NODE_TYPE) {
        root =
            new PagedBPlusTreeInternalNode<>(
                rootPageId, bufferPool, keySerializer, valueSerializer, comparator, order);
        root.deserialize(data);
      } else {
        // Empty file or uninitialized page - create new leaf node
        root =
            new PagedBPlusTreeLeafNode<>(
                rootPageId, bufferPool, keySerializer, valueSerializer, comparator, order);
        // Do NOT deserialize - this is a fresh node
      }
    } finally {
      bufferPool.unpinPage(rootPageId, false);
    }

    return root;
  }

  /** Saves the root page ID to the metadata page (page 0). */
  private void saveRootPageId(PageId metadataPageId) throws IOException {
    var page = bufferPool.fetchPage(metadataPageId);
    try {
      // Simple format: just store the root page ID as a long
      var buffer = java.nio.ByteBuffer.allocate(8);
      buffer.putLong(rootPageId.value());
      page.writeData(buffer.array());
    } finally {
      bufferPool.unpinPage(metadataPageId, true);
    }
  }

  /** Loads the root page ID from the metadata page (page 0). */
  private PageId loadRootPageId() throws IOException {
    var metadataPageId = new PageId(0);
    var page = bufferPool.fetchPage(metadataPageId);
    try {
      var data = page.readData();
      if (data.length >= 8) {
        var buffer = java.nio.ByteBuffer.wrap(data);
        var rootPageValue = buffer.getLong();
        return new PageId(rootPageValue);
      } else {
        // Fallback to page 1 if metadata is corrupted
        return new PageId(1);
      }
    } finally {
      bufferPool.unpinPage(metadataPageId, false);
    }
  }

  /** Updates the root page ID in the metadata page. */
  private void updateRootPageId() throws IOException {
    var metadataPageId = new PageId(0);
    saveRootPageId(metadataPageId);
  }

  /**
   * Finds the first leaf node that might contain the given key.
   *
   * @param key the key to search for
   * @return the first leaf node in the search path
   * @throws IOException if an I/O error occurs
   */
  private PagedBPlusTreeLeafNode<K, V> findFirstLeaf(K key) throws IOException {
    var current = loadRootNode();

    while (!current.isLeaf()) {
      var internalNode = (PagedBPlusTreeInternalNode<K, V>) current;
      var childIndex = findChildIndex(current, key);
      var childPageId = internalNode.getChildPageIds().get(childIndex);

      // Load the child node
      var page = bufferPool.fetchPage(childPageId);
      try {
        var data = page.readData();
        if (data.length > 0 && data[0] == PagedBPlusTreeNode.LEAF_NODE_TYPE) {
          current =
              new PagedBPlusTreeLeafNode<>(
                  childPageId, bufferPool, keySerializer, valueSerializer, comparator, order);
          current.deserialize(data);
        } else if (data.length > 0 && data[0] == PagedBPlusTreeNode.INTERNAL_NODE_TYPE) {
          current =
              new PagedBPlusTreeInternalNode<>(
                  childPageId, bufferPool, keySerializer, valueSerializer, comparator, order);
          current.deserialize(data);
        } else {
          // Uninitialized page - create new leaf node
          current =
              new PagedBPlusTreeLeafNode<>(
                  childPageId, bufferPool, keySerializer, valueSerializer, comparator, order);
        }
      } finally {
        bufferPool.unpinPage(childPageId, false);
      }
    }

    return (PagedBPlusTreeLeafNode<K, V>) current;
  }

  /**
   * Finds the child index for a given key in a node.
   *
   * @param node the node to search in
   * @param key the key to find the child index for
   * @return the child index
   */
  private int findChildIndex(PagedBPlusTreeNode<K, V> node, K key) {
    var index = 0;
    var keys = node.getKeys();
    while (index < keys.size() && comparator.compare(key, keys.get(index)) >= 0) {
      index++;
    }
    return index;
  }
}
