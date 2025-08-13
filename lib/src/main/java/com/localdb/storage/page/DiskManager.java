package com.localdb.storage.page;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages disk I/O operations for pages. Handles reading and writing pages to/from disk storage.
 * Thread-safe through read-write locking.
 */
public final class DiskManager implements AutoCloseable {

  private final Path filePath;
  private final FileChannel fileChannel;
  private final ReentrantReadWriteLock lock;
  private PageId nextPageId;

  /**
   * Creates a new disk manager for the specified file.
   *
   * @param filePath the path to the database file
   * @throws IOException if the file cannot be opened
   */
  public DiskManager(Path filePath) throws IOException {
    this.filePath = filePath;
    this.fileChannel =
        FileChannel.open(
            filePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    this.lock = new ReentrantReadWriteLock();
    this.nextPageId = calculateNextPageId();
  }

  /**
   * Reads a page from disk.
   *
   * @param pageId the ID of the page to read
   * @return the page data
   * @throws IOException if an I/O error occurs
   * @throws IllegalArgumentException if pageId is invalid
   */
  public Page readPage(PageId pageId) throws IOException {
    if (!pageId.isValid()) {
      throw new IllegalArgumentException("Invalid page ID: " + pageId);
    }

    lock.readLock().lock();
    try {
      var position = pageId.value() * Page.PAGE_SIZE;
      var buffer = java.nio.ByteBuffer.allocate(Page.PAGE_SIZE);

      var bytesRead = fileChannel.read(buffer, position);
      if (bytesRead == -1) {
        // Page doesn't exist on disk, return empty page
        return new Page(pageId);
      }

      buffer.flip();
      var data = new byte[buffer.remaining()];
      buffer.get(data);

      return new Page(pageId, data);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Writes a page to disk.
   *
   * @param page the page to write
   * @throws IOException if an I/O error occurs
   * @throws IllegalArgumentException if page ID is invalid
   */
  public void writePage(Page page) throws IOException {
    var pageId = page.getPageId();
    if (!pageId.isValid()) {
      throw new IllegalArgumentException("Invalid page ID: " + pageId);
    }

    lock.writeLock().lock();
    try {
      var position = pageId.value() * Page.PAGE_SIZE;
      var data = page.readData();
      var buffer = java.nio.ByteBuffer.allocate(Page.PAGE_SIZE);
      buffer.put(data);

      // Pad with zeros if data is smaller than page size
      while (buffer.hasRemaining()) {
        buffer.put((byte) 0);
      }

      buffer.flip();
      fileChannel.write(buffer, position);
      fileChannel.force(false); // Sync to disk

      page.markClean();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Allocates a new page ID.
   *
   * @return a new unique page ID
   */
  public PageId allocatePageId() {
    lock.writeLock().lock();
    try {
      var newPageId = nextPageId;
      nextPageId = nextPageId.next();
      return newPageId;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Gets the current file size in bytes.
   *
   * @return the file size
   * @throws IOException if an I/O error occurs
   */
  public long getFileSize() throws IOException {
    lock.readLock().lock();
    try {
      return fileChannel.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Forces any buffered data to be written to disk.
   *
   * @throws IOException if an I/O error occurs
   */
  public void sync() throws IOException {
    lock.readLock().lock();
    try {
      fileChannel.force(true);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {
    lock.writeLock().lock();
    try {
      if (fileChannel.isOpen()) {
        fileChannel.close();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Calculates the next available page ID based on file size.
   *
   * @return the next page ID
   * @throws IOException if an I/O error occurs
   */
  private PageId calculateNextPageId() throws IOException {
    var fileSize = fileChannel.size();
    var pageCount = fileSize / Page.PAGE_SIZE;
    return new PageId(pageCount);
  }

  /**
   * Gets the path to the database file.
   *
   * @return the file path
   */
  public Path getFilePath() {
    return filePath;
  }
}
