package com.cloudera.sa.copybook.common;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class IOUtils {

  /**
   * writes byte array to a fixed record length file
   *
   * copied and modified from JRecord: net.sf.JRecord.zTest.Common.IO class
   *
   * @param name  file name
   * @param bytes data to write to the file
   * @throws java.io.IOException any IO errors
   */
  public static void writeFbFile(String name, byte[][] bytes)
      throws IOException {
    int i;
    FileOutputStream f = new FileOutputStream(name);
    BufferedOutputStream outputStream = new BufferedOutputStream(f);

    try {
      for (i = 0; i < bytes.length; i++) {
        outputStream.write(bytes[i]);
      }
    } finally {
      outputStream.close();
      f.close();
    }
  }

  /**
   * writes byte array to a  VB file
   *
   * copied and modified from JRecord: net.sf.JRecord.zTest.Common.IO class
   *
   * @param name file name
   * @param bytes data to write to the file
   *
   * @throws IOException any IO errors
   */
  public static void writeVbFile(String name, byte[][] bytes)
      throws IOException  {
    int i;
    byte[] len = new byte[4];
    FileOutputStream f = new FileOutputStream(name);
    BufferedOutputStream outputStream = new BufferedOutputStream(f);

    // RDW is 2 bytes length followed by two zero bytes
    len[0] = 0;
    len[2] = 0;
    len[3] = 0;

    try {
      for (i = 0; i < bytes.length; i++) {
        len[1] = (byte) (bytes[i].length + 4);
        outputStream.write(len);
        outputStream.write(bytes[i]);
      }
    } finally {
      outputStream.close();
      f.close();
    }
  }


}
