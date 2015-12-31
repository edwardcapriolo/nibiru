package io.teknek.nibiru.engine;

public class MemtablePair<Left, Right> {
  private Left left;
  private Right right;

  public MemtablePair(Left l, Right r) {
    left = l;
    right = r;
  }

  public Left getLeft() {
    return left;
  }

  public void setLeft(Left left) {
    this.left = left;
  }

  public Right getRight() {
    return right;
  }

  public void setRight(Right right) {
    this.right = right;
  }
}
