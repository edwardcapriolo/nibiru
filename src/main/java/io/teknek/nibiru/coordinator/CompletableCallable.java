package io.teknek.nibiru.coordinator;

public class CompletableCallable {
  protected volatile boolean complete;

  public boolean isComplete() {
    return complete;
  }

  public void setComplete(boolean complete) {
    this.complete = complete;
  }


}
