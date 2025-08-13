package com.localdb.storage.page;

/**
 * Represents a unique identifier for a page in the storage system. PageIds are used to reference
 * pages on disk and in the buffer pool.
 *
 * @param value the unique page identifier value
 */
public record PageId(long value) {

  /** The invalid page ID constant, used to represent null references. */
  public static final PageId INVALID = new PageId(-1);

  /** The root page ID constant. */
  public static final PageId ROOT = new PageId(0);

  /**
   * Creates a new PageId with the specified value.
   *
   * @param value the page identifier value
   * @throws IllegalArgumentException if value is negative (except for INVALID)
   */
  public PageId {
    if (value < -1) {
      throw new IllegalArgumentException("Page ID must be non-negative or -1 for INVALID");
    }
  }

  /**
   * Checks if this page ID is valid.
   *
   * @return true if this is not the INVALID page ID
   */
  public boolean isValid() {
    return !equals(INVALID);
  }

  /**
   * Returns the next page ID in sequence.
   *
   * @return a new PageId with value + 1
   * @throws IllegalStateException if this is the INVALID page ID
   */
  public PageId next() {
    if (!isValid()) {
      throw new IllegalStateException("Cannot get next of INVALID page ID");
    }
    return new PageId(value + 1);
  }
}
