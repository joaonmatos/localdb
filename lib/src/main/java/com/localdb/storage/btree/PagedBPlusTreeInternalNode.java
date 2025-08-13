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
 * An internal node in a page-based B+Tree implementation. Stores keys and child page references for
 * navigation within the tree.
 *
 * @param <K> the type of keys
 * @param <V> the type of values (only used for typing compatibility)
 */
public final class PagedBPlusTreeInternalNode<K, V> extends PagedBPlusTreeNode<K, V> {

  private List<PageId> childPageIds;

  /**
   * Creates a new paged internal node.
   *
   * @param pageId the page ID for this node
   * @param bufferPool the buffer pool for page management
   * @param keySerializer serialization strategy for keys
   * @param valueSerializer serialization strategy for values
   * @param comparator comparator for keys
   * @param maxKeys maximum number of keys per node
   */
  public PagedBPlusTreeInternalNode(
      PageId pageId,
      BufferPool bufferPool,
      SerializationStrategy<K> keySerializer,
      SerializationStrategy<V> valueSerializer,
      Comparator<K> comparator,
      int maxKeys) {
    super(pageId, bufferPool, keySerializer, valueSerializer, comparator, maxKeys, false);
    this.childPageIds = new ArrayList<>();
  }

  @Override
  public SearchResult<V> search(K key) throws IOException {
    var childIndex = findChildIndex(key);
    var child = loadChild(childIndex);
    return child.search(key);
  }

  @Override
  public PagedInsertResult<K, V> insert(K key, V value) throws IOException {
    var childIndex = findChildIndex(key);
    var child = loadChild(childIndex);
    var result = child.insert(key, value);

    // Always save the child after modification
    child.savePage();

    if (result.isSplitOccurred()) {
      // Insert the promoted key and new child reference
      keys.add(childIndex, result.getPromotedKey());
      childPageIds.add(childIndex + 1, result.getNewNode().getPageId());
      markDirty();

      // Save the new node from split
      result.getNewNode().savePage();

      // Check if this node needs to split
      if (keys.size() > maxKeys) {
        return split();
      }

      // Save the current internal node since it was modified
      savePage();
    }

    return PagedInsertResult.success();
  }

  @Override
  public PagedDeleteResult<K, V> delete(K key) throws IOException {
    var childIndex = findChildIndex(key);
    var child = loadChild(childIndex);
    var result = child.delete(key);

    // Always save the child after modification
    child.savePage();

    if (result.isDeleted() && result.isUnderflow()) {
      // For now, ignore underflow to avoid complex merging issues
      // This will result in a less balanced tree but should work correctly
      // return handleChildUnderflow(childIndex);
    }

    return result;
  }

  /**
   * Finds the index of the child that should contain the given key.
   *
   * @param key the key to search for
   * @return the child index
   */
  private int findChildIndex(K key) {
    var index = 0;
    while (index < keys.size() && comparator.compare(key, keys.get(index)) >= 0) {
      index++;
    }
    return index;
  }

  /**
   * Loads a child node at the specified index.
   *
   * @param childIndex the index of the child to load
   * @return the loaded child node
   * @throws IOException if an I/O error occurs
   */
  public PagedBPlusTreeNode<K, V> loadChild(int childIndex) throws IOException {
    var childPageId = childPageIds.get(childIndex);

    // We need to determine if the child is a leaf or internal node
    var page = bufferPool.fetchPage(childPageId);
    PagedBPlusTreeNode<K, V> child;

    try {
      var data = page.readData();
      if (data.length > 0 && data[0] == LEAF_NODE_TYPE) {
        child =
            new PagedBPlusTreeLeafNode<>(
                childPageId, bufferPool, keySerializer, valueSerializer, comparator, maxKeys);
        child.deserialize(data);
      } else if (data.length > 0 && data[0] == INTERNAL_NODE_TYPE) {
        child =
            new PagedBPlusTreeInternalNode<>(
                childPageId, bufferPool, keySerializer, valueSerializer, comparator, maxKeys);
        child.deserialize(data);
      } else {
        // Uninitialized page - create new leaf node
        child =
            new PagedBPlusTreeLeafNode<>(
                childPageId, bufferPool, keySerializer, valueSerializer, comparator, maxKeys);
      }
    } finally {
      bufferPool.unpinPage(childPageId, false);
    }

    return child;
  }

  /**
   * Splits this internal node into two nodes.
   *
   * @return the insert result with the new node and promoted key
   * @throws IOException if an I/O error occurs
   */
  private PagedInsertResult<K, V> split() throws IOException {
    var mid = keys.size() / 2;

    // Create new internal node
    var newInternalPage = bufferPool.newPage();
    var newInternal =
        new PagedBPlusTreeInternalNode<>(
            newInternalPage.getPageId(),
            bufferPool,
            keySerializer,
            valueSerializer,
            comparator,
            maxKeys);

    // The middle key is promoted to the parent
    var promotedKey = keys.get(mid);

    // Move half the keys and children to the new node
    newInternal.keys.addAll(keys.subList(mid + 1, keys.size()));
    newInternal.childPageIds.addAll(childPageIds.subList(mid + 1, childPageIds.size()));

    // Remove moved keys and children from this node
    keys.subList(mid, keys.size()).clear();
    childPageIds.subList(mid + 1, childPageIds.size()).clear();

    // Mark both nodes as dirty
    markDirty();
    newInternal.markDirty();

    // Save both nodes
    savePage();
    newInternal.savePage();

    return PagedInsertResult.split(promotedKey, newInternal);
  }

  /**
   * Handles underflow in a child node by borrowing or merging.
   *
   * @param childIndex the index of the underflowing child
   * @return the delete result
   * @throws IOException if an I/O error occurs
   */
  private PagedDeleteResult<K, V> handleChildUnderflow(int childIndex) throws IOException {
    // For now, we'll implement a simple strategy that doesn't merge
    // In a full implementation, we would try borrowing first, then merge only if safe

    // Instead of merging, just remove the underflowing child
    // This is not optimal but prevents page size overflow
    if (childIndex > 0) {
      // Remove the separator key and the underflowing child
      keys.remove(childIndex - 1);
      childPageIds.remove(childIndex);
    } else if (childIndex < childPageIds.size() - 1) {
      // Remove the separator key and the underflowing child
      keys.remove(childIndex);
      childPageIds.remove(childIndex);
    }

    markDirty();

    if (isUnderflow()) {
      return PagedDeleteResult.underflow(keys.isEmpty() ? null : keys.get(0));
    }

    return PagedDeleteResult.success();
  }

  /**
   * Merges a child with its left sibling.
   *
   * @param childIndex the index of the child to merge
   * @throws IOException if an I/O error occurs
   */
  private void mergeWithLeftSibling(int childIndex) throws IOException {
    var leftChildPageId = childPageIds.get(childIndex - 1);
    var rightChildPageId = childPageIds.get(childIndex);

    var leftChild = loadChild(childIndex - 1);
    var rightChild = loadChild(childIndex);

    if (leftChild.isLeaf() && rightChild.isLeaf()) {
      // Merge leaf nodes
      var leftLeaf = (PagedBPlusTreeLeafNode<K, V>) leftChild;
      var rightLeaf = (PagedBPlusTreeLeafNode<K, V>) rightChild;

      // Move all keys and values from right to left
      leftLeaf.getKeys().addAll(rightLeaf.getKeys());
      leftLeaf.getValues().addAll(rightLeaf.getValues());

      // Update next pointer
      leftLeaf.setNextLeafPageId(rightLeaf.getNextLeafPageId());

      leftLeaf.markDirty();
      leftLeaf.savePage();
    } else if (!leftChild.isLeaf() && !rightChild.isLeaf()) {
      // Merge internal nodes
      var leftInternal = (PagedBPlusTreeInternalNode<K, V>) leftChild;
      var rightInternal = (PagedBPlusTreeInternalNode<K, V>) rightChild;

      // Add the separator key from parent
      leftInternal.getKeys().add(keys.get(childIndex - 1));

      // Move all keys and child pointers from right to left
      leftInternal.getKeys().addAll(rightInternal.getKeys());
      leftInternal.getChildPageIds().addAll(rightInternal.getChildPageIds());

      leftInternal.markDirty();
      leftInternal.savePage();
    }

    // Remove the separator key and right child pointer from parent
    keys.remove(childIndex - 1);
    childPageIds.remove(childIndex);
    markDirty();

    // TODO: Free the unused page (rightChildPageId)
  }

  /**
   * Merges a child with its right sibling.
   *
   * @param childIndex the index of the child to merge
   * @throws IOException if an I/O error occurs
   */
  private void mergeWithRightSibling(int childIndex) throws IOException {
    var leftChildPageId = childPageIds.get(childIndex);
    var rightChildPageId = childPageIds.get(childIndex + 1);

    var leftChild = loadChild(childIndex);
    var rightChild = loadChild(childIndex + 1);

    if (leftChild.isLeaf() && rightChild.isLeaf()) {
      // Merge leaf nodes
      var leftLeaf = (PagedBPlusTreeLeafNode<K, V>) leftChild;
      var rightLeaf = (PagedBPlusTreeLeafNode<K, V>) rightChild;

      // Move all keys and values from right to left
      leftLeaf.getKeys().addAll(rightLeaf.getKeys());
      leftLeaf.getValues().addAll(rightLeaf.getValues());

      // Update next pointer
      leftLeaf.setNextLeafPageId(rightLeaf.getNextLeafPageId());

      leftLeaf.markDirty();
      leftLeaf.savePage();
    } else if (!leftChild.isLeaf() && !rightChild.isLeaf()) {
      // Merge internal nodes
      var leftInternal = (PagedBPlusTreeInternalNode<K, V>) leftChild;
      var rightInternal = (PagedBPlusTreeInternalNode<K, V>) rightChild;

      // Add the separator key from parent
      leftInternal.getKeys().add(keys.get(childIndex));

      // Move all keys and child pointers from right to left
      leftInternal.getKeys().addAll(rightInternal.getKeys());
      leftInternal.getChildPageIds().addAll(rightInternal.getChildPageIds());

      leftInternal.markDirty();
      leftInternal.savePage();
    }

    // Remove the separator key and right child pointer from parent
    keys.remove(childIndex);
    childPageIds.remove(childIndex + 1);
    markDirty();

    // TODO: Free the unused page (rightChildPageId)
  }

  /**
   * Gets the child page IDs.
   *
   * @return the list of child page IDs
   */
  public List<PageId> getChildPageIds() {
    return childPageIds;
  }

  /**
   * Adds a child page ID.
   *
   * @param childPageId the child page ID to add
   */
  public void addChildPageId(PageId childPageId) {
    childPageIds.add(childPageId);
    markDirty();
  }

  @Override
  protected void serializeNodeSpecificData(DataOutputStream dos) throws IOException {
    // Write child page IDs
    for (var childPageId : childPageIds) {
      dos.writeLong(childPageId.value());
    }
  }

  @Override
  protected void deserializeNodeSpecificData(DataInputStream dis) throws IOException {
    // Read child page IDs (number of children = number of keys + 1)
    childPageIds.clear();
    var numChildren = keys.size() + 1;
    for (var i = 0; i < numChildren; i++) {
      var childPageIdValue = dis.readLong();
      childPageIds.add(new PageId(childPageIdValue));
    }
  }
}
