package io.teknek.nibiru.client;

@SuppressWarnings("serial")
public class ClientException extends Exception {

  public ClientException() {
    super();
  }

  public ClientException(String message, Throwable cause, boolean enableSuppression,
          boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public ClientException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClientException(String message) {
    super(message);
  }

  public ClientException(Throwable cause) {
    super(cause);
  }

}
