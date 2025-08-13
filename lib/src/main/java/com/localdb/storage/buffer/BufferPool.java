package com.localdb.storage.buffer;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.localdb.storage.page.DiskManager;
import com.localdb.storage.page.Page;
import com.localdb.storage.page.PageId;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A buffer pool implementation that manages pages in memory using LRU eviction. The buffer pool
 * caches frequently accessed pages to reduce disk I/O operations. Thread-safe implementation
 * supporting concurrent access.
 */
public final class BufferPool implements AutoCloseable {

  /** Default buffer pool size (1000 pages = ~4MB). */
  public static final int DEFAULT_POOL_SIZE = 1000;

  private final DiskManager diskManager;
  private final int poolSize;
  private final Cache<PageId, Page> pageCache;
  private final Set<PageId> knownPages; // Track all pages that have been created/fetched
  private final ReentrantReadWriteLock lock;

  /**
   * Creates a new buffer pool with the specified disk manager and pool size.
   *
   * @param diskManager the disk manager for I/O operations
   * @param poolSize the maximum number of pages to keep in memory
   * @throws IllegalArgumentException if poolSize is not positive
   */
  public BufferPool(DiskManager diskManager, int poolSize) {
    if (poolSize <= 0) {
      throw new IllegalArgumentException("Pool size must be positive");
    }

    this.diskManager = diskManager;
    this.poolSize = poolSize;
    this.knownPages = ConcurrentHashMap.newKeySet();
    this.pageCache =
        Caffeine.newBuilder()
            .maximumSize(poolSize)
            .removalListener(
                (PageId key, Page value, com.github.benmanes.caffeine.cache.RemovalCause cause) -> {
                  if (value != null && value.isDirty()) {
                    try {
                      // Save dirty page to disk before eviction
                      diskManager.writePage(value);
                      value.markClean(); // Mark as clean after writing
                    } catch (IOException e) {
                      // Log error but don't throw to avoid breaking cache operations
                      System.err.println(
                          "Error saving evicted page " + key + ": " + e.getMessage());
                    }
                  }
                })
            .build();
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Creates a new buffer pool with the default pool size.
   *
   * @param diskManager the disk manager for I/O operations
   */
  public BufferPool(DiskManager diskManager) {
    this(diskManager, DEFAULT_POOL_SIZE);
  }

  /**
   * Fetches a page from the buffer pool, loading it from disk if necessary. The page is pinned and
   * must be unpinned when no longer needed.
   *
   * @param pageId the ID of the page to fetch
   * @return the requested page
   * @throws IOException if an I/O error occurs
   * @throws IllegalArgumentException if pageId is invalid
   */
  public Page fetchPage(PageId pageId) throws IOException {
    if (!pageId.isValid()) {
      throw new IllegalArgumentException("Invalid page ID: " + pageId);
    }

    var page = pageCache.getIfPresent(pageId);
    if (page != null) {
      page.pin();
      return page;
    }

    // Page not in buffer, need to load from disk
    lock.writeLock().lock();
    try {
      // Double-check in case another thread loaded it
      page = pageCache.getIfPresent(pageId);
      if (page != null) {
        page.pin();
        return page;
      }

      // Load page from disk
      page = diskManager.readPage(pageId);
      page.pin();

      // Track this page as known
      knownPages.add(pageId);

      // Add to cache (Caffeine handles eviction automatically)
      pageCache.put(pageId, page);

      return page;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Creates a new page with a newly allocated page ID. The page is pinned and must be unpinned when
   * no longer needed.
   *
   * @return the new page
   * @throws IOException if an I/O error occurs
   * @throws IllegalStateException if the buffer pool is full and no pages can be evicted
   */
  public Page newPage() throws IOException {
    // Check if buffer is full and all pages are pinned
    if (pageCache.estimatedSize() >= poolSize) {
      boolean canEvict = false;
      for (var page : pageCache.asMap().values()) {
        if (!page.isPinned()) {
          canEvict = true;
          break;
        }
      }
      if (!canEvict) {
        throw new IllegalStateException("Buffer pool is full and all pages are pinned");
      }
    }

    var pageId = diskManager.allocatePageId();
    var page = new Page(pageId);
    page.pin();

    // Track this page as known
    knownPages.add(pageId);

    // Add to cache (Caffeine handles eviction automatically)
    pageCache.put(pageId, page);

    // Force cleanup to ensure size constraints are maintained
    pageCache.cleanUp();

    return page;
  }

  /**
   * Unpins a page in the buffer pool. When a page's pin count reaches 0, it becomes eligible for
   * eviction.
   *
   * @param pageId the ID of the page to unpin
   * @param dirty whether the page has been modified
   * @throws IllegalArgumentException if the page was never created or fetched
   */
  public void unpinPage(PageId pageId, boolean dirty) {
    // Check if this page was ever known to the buffer pool
    if (!knownPages.contains(pageId)) {
      throw new IllegalArgumentException("Page not in buffer pool: " + pageId);
    }

    var page = pageCache.getIfPresent(pageId);
    if (page == null) {
      // Page was evicted from buffer pool - this is okay, just ignore
      return;
    }

    page.unpin();
    if (dirty) {
      page.markDirty();
    }
  }

  /**
   * Forces all dirty pages to be written to disk.
   *
   * @throws IOException if an I/O error occurs
   */
  public void flushAllPages() throws IOException {
    for (var page : pageCache.asMap().values()) {
      if (page.isDirty()) {
        diskManager.writePage(page);
        page.markClean();
      }
    }
  }

  /**
   * Flushes a specific page to disk if it's dirty.
   *
   * @param pageId the ID of the page to flush
   * @throws IOException if an I/O error occurs
   * @throws IllegalArgumentException if the page is not in the buffer pool
   */
  public void flushPage(PageId pageId) throws IOException {
    var page = pageCache.getIfPresent(pageId);
    if (page == null) {
      throw new IllegalArgumentException("Page not in buffer pool: " + pageId);
    }

    if (page.isDirty()) {
      diskManager.writePage(page);
      page.markClean();
    }
  }

  /**
   * Gets the current number of pages in the buffer pool.
   *
   * @return the number of cached pages
   */
  public int getSize() {
    pageCache.cleanUp(); // Force cleanup to get accurate size
    return (int) pageCache.estimatedSize();
  }

  /**
   * Gets the maximum capacity of the buffer pool.
   *
   * @return the pool size
   */
  public int getCapacity() {
    return poolSize;
  }

  /**
   * Gets statistics about the buffer pool usage.
   *
   * @return a string describing buffer pool statistics
   */
  public String getBufferPoolStats() {
    return String.format("Buffer Pool: %d/%d pages", (int) pageCache.estimatedSize(), poolSize);
  }

  @Override
  public void close() throws IOException {
    flushAllPages();
    pageCache.invalidateAll();
  }
}
