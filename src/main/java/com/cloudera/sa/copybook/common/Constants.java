package com.cloudera.sa.copybook.common;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

public class Constants {
  public static final String COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF = "copybook.inputformat.cbl.hdfs.path";
  public static final String COPYBOOK_INPUTFORMAT_FIELD_DELIMITER = "copybook.inputformat.field.delim";
  public static final String COPYBOOK_INPUTFORMAT_FILE_STRUCTURE = "copybook.inputformat.input.filestructure";

  public static final String DEFAULT_OUTPUT_DELIMITER = new Character((char) 0x01).toString();
  public static final int DEFAULT_FILE_STRUCTURE = net.sf.JRecord.Common.Constants.IO_FIXED_LENGTH;

  public static final List<Integer> SUPPORTED_FILE_STRUCTURES = Lists.newArrayList(
    net.sf.JRecord.Common.Constants.IO_FIXED_LENGTH,
    net.sf.JRecord.Common.Constants.IO_VB
  );

}
