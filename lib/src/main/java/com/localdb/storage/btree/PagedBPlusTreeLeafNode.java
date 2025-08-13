package com.localdb.storage.btree;

import com.localdb.serialization.SerializationStrategy;
import com.localdb.storage.Comparator;
import com.localdb.storage.buffer.BufferPool;
import com.localdb.storage.page.PageId;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A leaf node in a page-based B+Tree implementation. Stores key-value pairs and maintains links to
 * sibling leaf nodes for range queries.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public final class PagedBPlusTreeLeafNode<K, V> extends PagedBPlusTreeNode<K, V> {

  private List<V> values;
  private PageId nextLeafPageId;

  /**
   * Creates a new paged leaf node.
   *
   * @param pageId the page ID for this node
   * @param bufferPool the buffer pool for page management
   * @param keySerializer serialization strategy for keys
   * @param valueSerializer serialization strategy for values
   * @param comparator comparator for keys
   * @param maxKeys maximum number of keys per node
   */
  public PagedBPlusTreeLeafNode(
      PageId pageId,
      BufferPool bufferPool,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator,
      int maxKeys) {
    super(pageId, bufferPool, keySerializer, valueSerializer, comparator, maxKeys, true);
    this.values = new ArrayList<>();
    this.nextLeafPageId = PageId.INVALID;
  }

  @Override
  public SearchResult<V> search(K key) throws IOException {
    var index = findKeyIndex(key);
    if (index < keys.size() && comparator.compare(keys.get(index), key) == 0) {
      return SearchResult.found(values.get(index));
    }
    return SearchResult.notFound();
  }

  @Override
  public PagedInsertResult<K, V> insert(K key, V value) throws IOException {
    var index = findKeyIndex(key);

    // Update existing key
    if (index < keys.size() && comparator.compare(keys.get(index), key) == 0) {
      values.set(index, value);
      markDirty();
      return PagedInsertResult.success();
    }

    // Insert new key-value pair
    keys.add(index, key);
    values.add(index, value);
    markDirty();

    // Check if split is needed
    if (keys.size() > maxKeys) {
      return split();
    }

    return PagedInsertResult.success();
  }

  @Override
  public PagedDeleteResult<K, V> delete(K key) throws IOException {
    var index = findKeyIndex(key);

    if (index >= keys.size() || comparator.compare(keys.get(index), key) != 0) {
      return PagedDeleteResult.notFound();
    }

    keys.remove(index);
    values.remove(index);
    markDirty();

    if (isUnderflow()) {
      return PagedDeleteResult.underflow(keys.isEmpty() ? null : keys.get(0));
    }

    return PagedDeleteResult.success();
  }

  /**
   * Splits this leaf node into two nodes.
   *
   * @return the insert result with the new node and promoted key
   * @throws IOException if an I/O error occurs
   */
  private PagedInsertResult<K, V> split() throws IOException {
    var mid = keys.size() / 2;

    // Create new leaf node
    var newLeafPage = bufferPool.newPage();
    var newLeaf =
        new PagedBPlusTreeLeafNode<>(
            newLeafPage.getPageId(),
            bufferPool,
            keySerializer,
            valueSerializer,
            comparator,
            maxKeys);

    // Move half the keys and values to the new node
    newLeaf.keys.addAll(keys.subList(mid, keys.size()));
    newLeaf.values.addAll(values.subList(mid, values.size()));

    // Remove moved keys and values from this node
    keys.subList(mid, keys.size()).clear();
    values.subList(mid, values.size()).clear();

    // Update leaf chain pointers
    newLeaf.nextLeafPageId = this.nextLeafPageId;
    this.nextLeafPageId = newLeaf.getPageId();

    // Mark both nodes as dirty
    markDirty();
    newLeaf.markDirty();

    // Save both nodes
    savePage();
    newLeaf.savePage();

    // Return the promoted key (first key of new node)
    return PagedInsertResult.split(newLeaf.keys.get(0), newLeaf);
  }

  /**
   * Gets the values stored in this leaf node.
   *
   * @return the list of values
   */
  public List<V> getValues() {
    return values;
  }

  /**
   * Gets the page ID of the next leaf node in the chain.
   *
   * @return the next leaf page ID, or PageId.INVALID if this is the last leaf
   */
  public PageId getNextLeafPageId() {
    return nextLeafPageId;
  }

  /**
   * Sets the page ID of the next leaf node in the chain.
   *
   * @param nextLeafPageId the next leaf page ID
   */
  public void setNextLeafPageId(PageId nextLeafPageId) {
    this.nextLeafPageId = nextLeafPageId;
    markDirty();
  }

  /**
   * Gets the next leaf node in the chain.
   *
   * @return the next leaf node, or null if this is the last leaf
   * @throws IOException if an I/O error occurs
   */
  public PagedBPlusTreeLeafNode<K, V> getNextLeaf() throws IOException {
    if (!nextLeafPageId.isValid()) {
      return null;
    }

    var nextLeaf =
        new PagedBPlusTreeLeafNode<>(
            nextLeafPageId, bufferPool, keySerializer, valueSerializer, comparator, maxKeys);
    nextLeaf.loadPage();
    return nextLeaf;
  }

  @Override
  protected void serializeNodeSpecificData(DataOutputStream dos) throws IOException {
    // Write next leaf page ID
    dos.writeLong(nextLeafPageId.value());

    // Write values
    for (var value : values) {
      var valueBytes = valueSerializer.serialize(value);
      dos.writeInt(valueBytes.length);
      dos.write(valueBytes);
    }
  }

  @Override
  protected void deserializeNodeSpecificData(DataInputStream dis) throws IOException {
    // Read next leaf page ID
    var nextPageIdValue = dis.readLong();
    this.nextLeafPageId = new PageId(nextPageIdValue);

    // Read values (number of values equals number of keys)
    values.clear();
    for (var i = 0; i < keys.size(); i++) {
      var valueLength = dis.readInt();
      var valueBytes = new byte[valueLength];
      dis.readFully(valueBytes);
      values.add(valueSerializer.deserialize(valueBytes));
    }
  }
}
