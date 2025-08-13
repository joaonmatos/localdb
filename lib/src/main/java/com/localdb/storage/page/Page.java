package com.localdb.storage.page;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a fixed-size page in the storage system. Pages are the basic unit of I/O and contain
 * raw byte data. Thread-safe through read-write locking.
 */
public final class Page {

  /** Standard page size in bytes (4KB). */
  public static final int PAGE_SIZE = 4096;

  private final PageId pageId;
  private final ByteBuffer data;
  private boolean dirty;
  private int pinCount;
  private final ReentrantReadWriteLock lock;

  /**
   * Creates a new page with the specified ID.
   *
   * @param pageId the unique identifier for this page
   */
  public Page(PageId pageId) {
    this.pageId = pageId;
    this.data = ByteBuffer.allocate(PAGE_SIZE);
    this.dirty = false;
    this.pinCount = 0;
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Creates a new page with the specified ID and initial data.
   *
   * @param pageId the unique identifier for this page
   * @param initialData the initial data for the page
   * @throws IllegalArgumentException if initialData is larger than PAGE_SIZE
   */
  public Page(PageId pageId, byte[] initialData) {
    this(pageId);
    if (initialData.length > PAGE_SIZE) {
      throw new IllegalArgumentException("Initial data exceeds page size");
    }
    data.put(initialData);
    data.flip();
  }

  /**
   * Gets the page ID.
   *
   * @return the page identifier
   */
  public PageId getPageId() {
    return pageId;
  }

  /**
   * Reads data from the page. Acquires a read lock during the operation.
   *
   * @return a copy of the page data
   */
  public byte[] readData() {
    lock.readLock().lock();
    try {
      var result = new byte[data.limit()];
      data.rewind();
      data.get(result);
      return result;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Writes data to the page. Acquires a write lock and marks the page as dirty.
   *
   * @param newData the data to write
   * @throws IllegalArgumentException if newData is larger than PAGE_SIZE
   */
  public void writeData(byte[] newData) {
    if (newData.length > PAGE_SIZE) {
      throw new IllegalArgumentException("Data exceeds page size");
    }

    lock.writeLock().lock();
    try {
      data.clear();
      data.put(newData);
      data.flip();
      dirty = true;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Checks if the page has been modified since the last flush.
   *
   * @return true if the page is dirty
   */
  public boolean isDirty() {
    lock.readLock().lock();
    try {
      return dirty;
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Marks the page as clean (not dirty). Should be called after successfully writing to disk. */
  public void markClean() {
    lock.writeLock().lock();
    try {
      dirty = false;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Marks the page as dirty (modified). Should be called when the page contents have been modified.
   */
  public void markDirty() {
    lock.writeLock().lock();
    try {
      dirty = true;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Increments the pin count for this page. Pinned pages cannot be evicted from the buffer pool.
   *
   * @return the new pin count
   */
  public int pin() {
    lock.writeLock().lock();
    try {
      return ++pinCount;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Decrements the pin count for this page.
   *
   * @return the new pin count
   * @throws IllegalStateException if pin count is already 0
   */
  public int unpin() {
    lock.writeLock().lock();
    try {
      if (pinCount <= 0) {
        throw new IllegalStateException("Cannot unpin page with pin count " + pinCount);
      }
      return --pinCount;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Gets the current pin count.
   *
   * @return the pin count
   */
  public int getPinCount() {
    lock.readLock().lock();
    try {
      return pinCount;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Checks if the page is pinned (pin count > 0).
   *
   * @return true if the page is pinned
   */
  public boolean isPinned() {
    return getPinCount() > 0;
  }
}
