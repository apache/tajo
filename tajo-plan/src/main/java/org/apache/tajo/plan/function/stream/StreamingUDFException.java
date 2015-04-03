package org.apache.tajo.plan.function.stream;

public class StreamingUDFException extends Exception {

  private String message;
  private String language;
  private Integer lineNumber;

  public StreamingUDFException() {
  }

  public StreamingUDFException(String message) {
    this.message = message;
  }

  public StreamingUDFException(String message, Integer lineNumber) {
    this.message = message;
    this.lineNumber = lineNumber;
  }

  public StreamingUDFException(String language, String message, Throwable cause) {
    super(cause);
    this.language = language;
    this.message = message + "\n" + cause.getMessage() + "\n";
  }

  public StreamingUDFException(String language, String message) {
    this(language, message, (Integer) null);
  }

  public StreamingUDFException(String language, String message, Integer lineNumber) {
    this.language = language;
    this.message = message;
    this.lineNumber = lineNumber;
  }

  public String getLanguage() {
    return language;
  }

  public Integer getLineNumber() {
    return lineNumber;
  }

  @Override
  public String getMessage() {
    return this.message;
  }

  @Override
  public String toString() {
    String s = getClass().getName();
    String message = getMessage();
    String lineNumber = this.getLineNumber() == null ? "" : "" + this.getLineNumber();
    return (message != null) ? (s + ": " + "LINE " + lineNumber + ": " + message) : s;
  }
}
