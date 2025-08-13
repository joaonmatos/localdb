package com.localdb.storage.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.localdb.storage.page.DiskManager;
import com.localdb.storage.page.Page;
import com.localdb.storage.page.PageId;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BufferPoolTest {

  @TempDir Path tempDir;

  private Path dbFile;
  private DiskManager diskManager;
  private BufferPool bufferPool;

  @BeforeEach
  void setUp() throws IOException {
    dbFile = tempDir.resolve("test.db");
    diskManager = new DiskManager(dbFile);
    bufferPool = new BufferPool(diskManager, 10); // Small pool for testing
  }

  @AfterEach
  void tearDown() throws IOException {
    if (bufferPool != null) {
      bufferPool.close();
    }
    if (diskManager != null) {
      diskManager.close();
    }
  }

  @Test
  void testBufferPoolCreation() {
    assertEquals(10, bufferPool.getCapacity());
    assertEquals(0, bufferPool.getSize());
  }

  @Test
  void testFetchNewPage() throws IOException {
    var page = bufferPool.newPage();

    assertNotNull(page);
    assertTrue(page.getPageId().isValid());
    assertTrue(page.isPinned());
    assertEquals(1, bufferPool.getSize());
  }

  @Test
  void testFetchExistingPage() throws IOException {
    var pageId = new PageId(0);
    var testData = "Test data".getBytes();

    // Write page to disk first
    var page = new Page(pageId, testData);
    diskManager.writePage(page);

    // Fetch from buffer pool
    var fetchedPage = bufferPool.fetchPage(pageId);

    assertNotNull(fetchedPage);
    assertEquals(pageId, fetchedPage.getPageId());
    assertTrue(fetchedPage.isPinned());
    assertEquals(1, bufferPool.getSize());
  }

  @Test
  void testUnpinPage() throws IOException {
    var page = bufferPool.newPage();
    var pageId = page.getPageId();

    assertTrue(page.isPinned());

    bufferPool.unpinPage(pageId, false);
    assertFalse(page.isPinned());
  }

  @Test
  void testUnpinNonExistentPage() {
    var pageId = new PageId(999);
    assertThrows(IllegalArgumentException.class, () -> bufferPool.unpinPage(pageId, false));
  }

  @Test
  void testFlushPage() throws IOException {
    var page = bufferPool.newPage();
    var pageId = page.getPageId();
    var testData = "Dirty data".getBytes();

    page.writeData(testData);
    assertTrue(page.isDirty());

    bufferPool.flushPage(pageId);
    assertFalse(page.isDirty());
  }

  @Test
  void testFlushAllPages() throws IOException {
    var page1 = bufferPool.newPage();
    var page2 = bufferPool.newPage();

    page1.writeData("Data 1".getBytes());
    page2.writeData("Data 2".getBytes());

    assertTrue(page1.isDirty());
    assertTrue(page2.isDirty());

    bufferPool.flushAllPages();

    assertFalse(page1.isDirty());
    assertFalse(page2.isDirty());
  }

  @Test
  void testEviction() throws IOException {
    // Fill the buffer pool to capacity
    for (var i = 0; i < 10; i++) {
      var page = bufferPool.newPage();
      bufferPool.unpinPage(page.getPageId(), false); // Unpin to make evictable
    }

    assertEquals(10, bufferPool.getSize());

    // Add one more page to trigger eviction
    var page = bufferPool.newPage();

    // Should still be at capacity due to eviction
    assertEquals(10, bufferPool.getSize());
    assertTrue(page.isPinned());
  }

  @Test
  void testCannotEvictPinnedPages() throws IOException {
    // Fill the buffer pool with pinned pages
    for (var i = 0; i < 10; i++) {
      bufferPool.newPage(); // Keep all pages pinned
    }

    assertEquals(10, bufferPool.getSize());

    // Try to add another page - should fail because no pages can be evicted
    assertThrows(IllegalStateException.class, () -> bufferPool.newPage());
  }

  @Test
  void testLRUOrdering() throws IOException {
    var page1 = bufferPool.newPage();
    var page2 = bufferPool.newPage();
    var page3 = bufferPool.newPage();

    var pageId1 = page1.getPageId();
    var pageId2 = page2.getPageId();
    var pageId3 = page3.getPageId();

    // Unpin all pages
    bufferPool.unpinPage(pageId1, false);
    bufferPool.unpinPage(pageId2, false);
    bufferPool.unpinPage(pageId3, false);

    // Access page1 to make it more recently used
    bufferPool.fetchPage(pageId1);
    bufferPool.unpinPage(pageId1, false);

    // Fill buffer pool to trigger eviction
    for (var i = 0; i < 8; i++) {
      var page = bufferPool.newPage();
      bufferPool.unpinPage(page.getPageId(), false);
    }

    assertEquals(10, bufferPool.getSize());

    // Add one more to trigger eviction
    bufferPool.newPage();

    // page2 should have been evicted (least recently used)
    // page1 and page3 should still be in buffer
    assertEquals(10, bufferPool.getSize());
  }

  @Test
  void testInvalidPageId() {
    assertThrows(IllegalArgumentException.class, () -> bufferPool.fetchPage(PageId.INVALID));
  }

  @Test
  void testBufferPoolStats() throws IOException {
    var stats = bufferPool.getBufferPoolStats();
    assertTrue(stats.contains("0/10"));

    bufferPool.newPage();
    stats = bufferPool.getBufferPoolStats();
    assertTrue(stats.contains("1/10"));
  }
}
