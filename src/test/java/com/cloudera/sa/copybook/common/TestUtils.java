package com.cloudera.sa.copybook.common;

import net.sf.JRecord.Common.RecordException;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.Log.TextLog;
import net.sf.JRecord.Numeric.ICopybookDialects;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class TestUtils {

  private static TestUtils instance;

  private static final String TMP_DIR = "target/testdir";
  private static final String FILE_PREFIX = "DTAR020";
  private static final String CBL_FILE = "src/test/resources/DTAR020.cbl";
  private static final int LINE_MULTIPLIER = 1000;
  private static final byte[][] DTAR020_LINES = {
      {-10, -7, -10, -7, -12, -15, -11, -8, 2, 12, 0, 64, 17, -116,
          40, 12, 0, 0, 0, 0, 28, 0, 0, 0, 0, 80, 28},
      {-10, -13, -10, -16, -12, -8, -16, -8, 2, 12, 0, 64, 17, -116,
          23, 12, 0, 0, 0, 0, 28, 0, 0, 0, 0, 72, 124},
      {-10, -14, -10, -8, -12, -10, -9, -15, 2, 12, 0, 64, 17, -116,
          104, 92, 0, 0, 0, 0, 28, 0, 0, 0, 6, -103, -100},
      {-10, -14, -10, -8, -12, -10, -9, -15, 2, 12, 0, 64, 17, -116,
          104, 92, 0, 0, 0, 0, 29, 0, 0, 0, 6, -103, -99},
      {-10, -12, -10, -13, -12, -12, -14, -7, 2, 12, 0, 64, 17, -116,
          -107, 124, 0, 0, 0, 0, 28, 0, 0, 0, 0, 57, -100}
  };

  private byte[][] ebcdicTestLines;

  private TestUtils() {

  }

  private byte[][] generateTestLines() {
    byte[][] testLines = new byte[LINE_MULTIPLIER * DTAR020_LINES.length][];
    for (int i = 0; i < LINE_MULTIPLIER * DTAR020_LINES.length; i++) {
      testLines[i] = DTAR020_LINES[i % DTAR020_LINES.length];
    }
    return testLines;
  }

  private void createIOFixedLengthFile() throws RecordException, IOException {
    IOUtils.writeFbFile(TMP_DIR + "/" + FILE_PREFIX + "_FB.bin", ebcdicTestLines);
  }

  private void createIOVbFile() throws RecordException, IOException {
    IOUtils.writeVbFile(TMP_DIR + "/" + FILE_PREFIX + "_VB.bin",
        ebcdicTestLines);
  }

  public String getTestFixedLengthFileLocation() {
    return TMP_DIR + "/" + FILE_PREFIX + "_FB.bin";
  }

  public String getTestVbFileLocation() {
    return TMP_DIR + "/" + FILE_PREFIX + "_VB.bin";
  }

  public String getCobolFileLocation() {
    return CBL_FILE;
  }

  public int getTestDataLength() {
    return ebcdicTestLines.length;
  }

  public void createTestFiles() throws Exception {
    ebcdicTestLines = generateTestLines();
    new File(TMP_DIR).mkdir();
    createIOFixedLengthFile();
    createIOVbFile();
  }

  public void removeTestFiles() {
    new File(TMP_DIR + "/" + FILE_PREFIX + "_FB.bin").delete();
    new File(TMP_DIR + "/" + FILE_PREFIX + "_VB.bin").delete();
    new File(TMP_DIR).delete();
  }

  public static synchronized TestUtils getInstance() {
    if (instance == null) {
      instance = new TestUtils();
    }
    return instance;
  }

}
