package com.localdb.storage.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PageIdTest {

  @Test
  void testValidPageId() {
    var pageId = new PageId(42);
    assertEquals(42, pageId.value());
    assertTrue(pageId.isValid());
  }

  @Test
  void testInvalidPageId() {
    assertEquals(-1, PageId.INVALID.value());
    assertFalse(PageId.INVALID.isValid());
  }

  @Test
  void testRootPageId() {
    assertEquals(0, PageId.ROOT.value());
    assertTrue(PageId.ROOT.isValid());
  }

  @Test
  void testPageIdEquality() {
    var pageId1 = new PageId(5);
    var pageId2 = new PageId(5);
    var pageId3 = new PageId(10);

    assertEquals(pageId1, pageId2);
    assertNotEquals(pageId1, pageId3);
    assertEquals(pageId1.hashCode(), pageId2.hashCode());
  }

  @Test
  void testNextPageId() {
    var pageId = new PageId(5);
    var nextPageId = pageId.next();

    assertEquals(6, nextPageId.value());
    assertTrue(nextPageId.isValid());
  }

  @Test
  void testNextOfInvalidPageId() {
    assertThrows(IllegalStateException.class, () -> PageId.INVALID.next());
  }

  @Test
  void testNegativePageIdValue() {
    assertThrows(IllegalArgumentException.class, () -> new PageId(-2));
  }

  @Test
  void testPageIdToString() {
    var pageId = new PageId(42);
    assertTrue(pageId.toString().contains("42"));
  }
}
