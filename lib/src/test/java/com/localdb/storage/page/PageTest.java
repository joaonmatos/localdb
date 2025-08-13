package com.localdb.storage.page;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PageTest {

  @Test
  void testPageCreation() {
    var pageId = new PageId(1);
    var page = new Page(pageId);

    assertEquals(pageId, page.getPageId());
    assertFalse(page.isDirty());
    assertEquals(0, page.getPinCount());
    assertFalse(page.isPinned());
  }

  @Test
  void testPageWithInitialData() {
    var pageId = new PageId(1);
    var initialData = "Hello World".getBytes();
    var page = new Page(pageId, initialData);

    var readData = page.readData();
    assertArrayEquals(initialData, readData);
  }

  @Test
  void testPageWithTooMuchInitialData() {
    var pageId = new PageId(1);
    var tooMuchData = new byte[Page.PAGE_SIZE + 1];

    assertThrows(IllegalArgumentException.class, () -> new Page(pageId, tooMuchData));
  }

  @Test
  void testWriteAndReadData() {
    var pageId = new PageId(1);
    var page = new Page(pageId);
    var testData = "Test data".getBytes();

    page.writeData(testData);
    assertTrue(page.isDirty());

    var readData = page.readData();
    assertArrayEquals(testData, readData);
  }

  @Test
  void testWriteTooMuchData() {
    var pageId = new PageId(1);
    var page = new Page(pageId);
    var tooMuchData = new byte[Page.PAGE_SIZE + 1];

    assertThrows(IllegalArgumentException.class, () -> page.writeData(tooMuchData));
  }

  @Test
  void testMarkClean() {
    var pageId = new PageId(1);
    var page = new Page(pageId);
    var testData = "Test data".getBytes();

    page.writeData(testData);
    assertTrue(page.isDirty());

    page.markClean();
    assertFalse(page.isDirty());
  }

  @Test
  void testPinning() {
    var pageId = new PageId(1);
    var page = new Page(pageId);

    assertEquals(0, page.getPinCount());
    assertFalse(page.isPinned());

    assertEquals(1, page.pin());
    assertEquals(1, page.getPinCount());
    assertTrue(page.isPinned());

    assertEquals(2, page.pin());
    assertEquals(2, page.getPinCount());

    assertEquals(1, page.unpin());
    assertEquals(1, page.getPinCount());
    assertTrue(page.isPinned());

    assertEquals(0, page.unpin());
    assertEquals(0, page.getPinCount());
    assertFalse(page.isPinned());
  }

  @Test
  void testUnpinningWithZeroPinCount() {
    var pageId = new PageId(1);
    var page = new Page(pageId);

    assertThrows(IllegalStateException.class, page::unpin);
  }
}
