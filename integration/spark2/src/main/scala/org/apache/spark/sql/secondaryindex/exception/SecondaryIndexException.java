
package org.apache.spark.sql.secondaryindex.exception;

/**
 * Exception class specific to SecondaryIndex creation
 */
public class SecondaryIndexException extends Exception {

  private String message;

  public SecondaryIndexException(String message) {
    super(message);
    this.message = message;
  }

  public SecondaryIndexException(String message, Throwable t) {
    super(message, t);
    this.message = message;
  }

  @Override public String getMessage() {
    return message;
  }
}
