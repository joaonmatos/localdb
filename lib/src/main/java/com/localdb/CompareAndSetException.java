package com.localdb;

/**
 * Exception thrown when a compare-and-set operation fails due to the current value not matching the
 * expected value at commit time.
 */
public class CompareAndSetException extends RuntimeException {
  private final Object key;
  private final Object expectedValue;
  private final Object actualValue;

  public CompareAndSetException(Object key, Object expectedValue, Object actualValue) {
    super(
        String.format(
            "Compare-and-set failed for key '%s': expected '%s' but found '%s'",
            key, expectedValue, actualValue));
    this.key = key;
    this.expectedValue = expectedValue;
    this.actualValue = actualValue;
  }

  public Object getKey() {
    return key;
  }

  public Object getExpectedValue() {
    return expectedValue;
  }

  public Object getActualValue() {
    return actualValue;
  }
}
