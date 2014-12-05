package io.teknek.nibiru.engine;

public class Index {

  private final BufferGroup bgIndex;

  public Index(BufferGroup bgIndex) {
    this.bgIndex = bgIndex;
  }

  public long findStartOffset() {
    return 0;
    /*
     * do { if (bgIndex.dst[bgIndex.currentIndex] == SSTable.END_ROW){ bgIndex.advanceIndex(); }
     * readHeader(bgIndex); StringBuilder token = readToken(bgIndex); long offset =
     * readIndexSize(bgIndex); } while (bgIndex.startOffset + bgIndex.currentIndex +1 <
     * indexChannel.size());
     */
  }
}
