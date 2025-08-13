package com.localdb.storage.page;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiskManagerTest {

  @TempDir Path tempDir;

  private Path dbFile;
  private DiskManager diskManager;

  @BeforeEach
  void setUp() throws IOException {
    dbFile = tempDir.resolve("test.db");
    diskManager = new DiskManager(dbFile);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (diskManager != null) {
      diskManager.close();
    }
  }

  @Test
  void testDiskManagerCreation() throws IOException {
    assertTrue(Files.exists(dbFile));
    assertEquals(dbFile, diskManager.getFilePath());
  }

  @Test
  void testReadNonExistentPage() throws IOException {
    var pageId = new PageId(0);
    var page = diskManager.readPage(pageId);

    assertEquals(pageId, page.getPageId());
    // Should return empty page for non-existent page
    var data = page.readData();
    assertTrue(data.length == 0 || (data.length > 0 && data[0] == 0));
  }

  @Test
  void testWriteAndReadPage() throws IOException {
    var pageId = new PageId(0);
    var testData = "Hello, World!".getBytes();
    var page = new Page(pageId, testData);

    diskManager.writePage(page);
    assertFalse(page.isDirty()); // Should be marked clean after write

    var readPage = diskManager.readPage(pageId);
    var readData = readPage.readData();

    // Data should be padded to page size with zeros
    assertEquals(Page.PAGE_SIZE, readData.length);

    // Check that our data is at the beginning
    var actualData = new byte[testData.length];
    System.arraycopy(readData, 0, actualData, 0, testData.length);
    assertArrayEquals(testData, actualData);
  }

  @Test
  void testAllocatePageId() {
    var pageId1 = diskManager.allocatePageId();
    var pageId2 = diskManager.allocatePageId();

    assertTrue(pageId1.isValid());
    assertTrue(pageId2.isValid());
    assertNotEquals(pageId1, pageId2);
    assertEquals(pageId1.value() + 1, pageId2.value());
  }

  @Test
  void testGetFileSize() throws IOException {
    var initialSize = diskManager.getFileSize();
    assertEquals(0, initialSize);

    var pageId = new PageId(0);
    var page = new Page(pageId, "test".getBytes());
    diskManager.writePage(page);

    var newSize = diskManager.getFileSize();
    assertEquals(Page.PAGE_SIZE, newSize);
  }

  @Test
  void testSync() throws IOException {
    // This test just ensures sync doesn't throw an exception
    assertDoesNotThrow(() -> diskManager.sync());
  }

  @Test
  void testWriteInvalidPageId() {
    var invalidPage = new Page(PageId.INVALID);
    assertThrows(IllegalArgumentException.class, () -> diskManager.writePage(invalidPage));
  }

  @Test
  void testReadInvalidPageId() {
    assertThrows(IllegalArgumentException.class, () -> diskManager.readPage(PageId.INVALID));
  }

  @Test
  void testMultiplePages() throws IOException {
    var page1 = new Page(new PageId(0), "Page 1".getBytes());
    var page2 = new Page(new PageId(1), "Page 2".getBytes());
    var page3 = new Page(new PageId(2), "Page 3".getBytes());

    diskManager.writePage(page1);
    diskManager.writePage(page2);
    diskManager.writePage(page3);

    var readPage1 = diskManager.readPage(new PageId(0));
    var readPage2 = diskManager.readPage(new PageId(1));
    var readPage3 = diskManager.readPage(new PageId(2));

    assertTrue(new String(readPage1.readData()).startsWith("Page 1"));
    assertTrue(new String(readPage2.readData()).startsWith("Page 2"));
    assertTrue(new String(readPage3.readData()).startsWith("Page 3"));
  }
}
