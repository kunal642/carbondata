
package org.apache.spark.sql.secondaryindex.exception;

/**
 *
 */
public class IndexTableExistException extends Exception {


  /**
   * default serial version ID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * The Error message.
   */
  private String msg;

  /**
   * Constructor
   *
   * @param msg The error message for this exception.
   */
  public IndexTableExistException(String msg) {
    super(msg);
    this.msg = msg;
  }

  /**
   * getMessage
   */
  @Override public String getMessage() {
    return this.msg;
  }
}
