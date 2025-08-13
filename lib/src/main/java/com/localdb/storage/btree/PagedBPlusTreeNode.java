package com.localdb.storage.btree;

import com.localdb.serialization.SerializationStrategy;
import com.localdb.storage.Comparator;
import com.localdb.storage.buffer.BufferPool;
import com.localdb.storage.page.PageId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for B+Tree nodes that are stored on pages. Provides common functionality for
 * page-based storage and serialization.
 */
public abstract class PagedBPlusTreeNode<K, V> {

  protected static final byte LEAF_NODE_TYPE = 1;
  protected static final byte INTERNAL_NODE_TYPE = 2;

  protected final PageId pageId;
  protected final BufferPool bufferPool;
  protected final SerializationStrategy<K> keySerializer;
  protected final SerializationStrategy<V> valueSerializer;
  protected final Comparator<K> comparator;
  protected final int maxKeys;
  protected boolean isLeaf;
  protected List<K> keys;
  protected boolean dirty;

  /**
   * Creates a new paged B+Tree node.
   *
   * @param pageId the page ID for this node
   * @param bufferPool the buffer pool for page management
   * @param keySerializer serialization strategy for keys
   * @param valueSerializer serialization strategy for values
   * @param comparator comparator for keys
   * @param maxKeys maximum number of keys per node
   * @param isLeaf whether this is a leaf node
   */
  protected PagedBPlusTreeNode(
      PageId pageId,
      BufferPool bufferPool,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator,
      int maxKeys,
      boolean isLeaf) {
    this.pageId = pageId;
    this.bufferPool = bufferPool;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.comparator = comparator;
    this.maxKeys = maxKeys;
    this.isLeaf = isLeaf;
    this.keys = new ArrayList<>();
    this.dirty = false;
  }

  /**
   * Gets the page ID for this node.
   *
   * @return the page ID
   */
  public PageId getPageId() {
    return pageId;
  }

  /**
   * Gets the keys in this node.
   *
   * @return the list of keys
   */
  public List<K> getKeys() {
    return keys;
  }

  /**
   * Checks if this is a leaf node.
   *
   * @return true if this is a leaf node
   */
  public boolean isLeaf() {
    return isLeaf;
  }

  /**
   * Checks if this node is full.
   *
   * @return true if the node contains the maximum number of keys
   */
  public boolean isFull() {
    return keys.size() >= maxKeys;
  }

  /**
   * Checks if this node has underflowed.
   *
   * @return true if the node has fewer than the minimum number of keys
   */
  public boolean isUnderflow() {
    return keys.size() < maxKeys / 2;
  }

  /** Marks this node as dirty, indicating it needs to be written to disk. */
  protected void markDirty() {
    this.dirty = true;
  }

  /**
   * Checks if this node is dirty.
   *
   * @return true if the node has been modified
   */
  public boolean isDirty() {
    return dirty;
  }

  /**
   * Finds the index where a key should be inserted or the index of an existing key.
   *
   * @param key the key to find
   * @return the index for the key
   */
  protected int findKeyIndex(K key) {
    var left = 0;
    var right = keys.size() - 1;

    while (left <= right) {
      var mid = (left + right) / 2;
      var cmp = comparator.compare(key, keys.get(mid));

      if (cmp == 0) {
        return mid;
      } else if (cmp < 0) {
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }

    return left;
  }

  /**
   * Serializes this node to bytes for storage on a page.
   *
   * @return the serialized node data
   * @throws IOException if serialization fails
   */
  public byte[] serialize() throws IOException {
    var baos = new ByteArrayOutputStream();
    var dos = new DataOutputStream(baos);

    // Write node type
    dos.writeByte(isLeaf ? LEAF_NODE_TYPE : INTERNAL_NODE_TYPE);

    // Write number of keys
    dos.writeInt(keys.size());

    // Write keys
    for (var key : keys) {
      var keyBytes = keySerializer.serialize(key);
      dos.writeInt(keyBytes.length);
      dos.write(keyBytes);
    }

    // Subclasses write additional data
    serializeNodeSpecificData(dos);

    return baos.toByteArray();
  }

  /**
   * Deserializes node data from bytes.
   *
   * @param data the serialized node data
   * @throws IOException if deserialization fails
   */
  public void deserialize(byte[] data) throws IOException {
    var bais = new ByteArrayInputStream(data);
    var dis = new DataInputStream(bais);

    // Read node type
    var nodeType = dis.readByte();
    this.isLeaf = (nodeType == LEAF_NODE_TYPE);

    // Read number of keys
    var keyCount = dis.readInt();
    keys.clear();

    // Read keys
    for (var i = 0; i < keyCount; i++) {
      var keyLength = dis.readInt();
      var keyBytes = new byte[keyLength];
      dis.readFully(keyBytes);
      keys.add(keySerializer.deserialize(keyBytes));
    }

    // Subclasses read additional data
    deserializeNodeSpecificData(dis);

    this.dirty = false;
  }

  /**
   * Saves this node to its page.
   *
   * @throws IOException if an I/O error occurs
   */
  public void savePage() throws IOException {
    if (!dirty) {
      return;
    }

    var page = bufferPool.fetchPage(pageId);
    try {
      var data = serialize();
      page.writeData(data);
    } finally {
      bufferPool.unpinPage(pageId, true);
    }

    this.dirty = false;
  }

  /**
   * Loads this node from its page.
   *
   * @throws IOException if an I/O error occurs
   */
  public void loadPage() throws IOException {
    var page = bufferPool.fetchPage(pageId);
    try {
      var data = page.readData();
      if (data.length > 0) {
        deserialize(data);
      }
    } finally {
      bufferPool.unpinPage(pageId, false);
    }
  }

  /**
   * Abstract method for subclasses to serialize their specific data.
   *
   * @param dos the output stream to write to
   * @throws IOException if serialization fails
   */
  protected abstract void serializeNodeSpecificData(DataOutputStream dos) throws IOException;

  /**
   * Abstract method for subclasses to deserialize their specific data.
   *
   * @param dis the input stream to read from
   * @throws IOException if deserialization fails
   */
  protected abstract void deserializeNodeSpecificData(DataInputStream dis) throws IOException;

  /**
   * Abstract method for searching within this node.
   *
   * @param key the key to search for
   * @return the search result
   * @throws IOException if an I/O error occurs
   */
  public abstract SearchResult<V> search(K key) throws IOException;

  /**
   * Abstract method for inserting into this node.
   *
   * @param key the key to insert
   * @param value the value to insert
   * @return the insert result
   * @throws IOException if an I/O error occurs
   */
  public abstract PagedInsertResult<K, V> insert(K key, V value) throws IOException;

  /**
   * Abstract method for deleting from this node.
   *
   * @param key the key to delete
   * @return the delete result
   * @throws IOException if an I/O error occurs
   */
  public abstract PagedDeleteResult<K, V> delete(K key) throws IOException;
}
