package com.cloudera.sa.copybook.common;

import com.google.common.base.Preconditions;
import net.sf.JRecord.Common.CommonBits;
import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.RecordException;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.Details.LineProvider;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.External.ToLayoutDetail;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.LineIOProvider;
import net.sf.JRecord.Numeric.Convert;
import net.sf.cb2xml.def.Cb2xmlConstants;

import java.io.IOException;
import java.io.InputStream;

public class CopybookIOUtils {

  public static ExternalRecord getExternalRecord(
    InputStream cblIs) throws RecordException {
    CommonBits.setDefaultCobolTextFormat(Cb2xmlConstants.USE_STANDARD_COLUMNS);
    CobolCopybookLoader copybookInt = new CobolCopybookLoader();
    return copybookInt.loadCopyBook(cblIs,
      "",
      CopybookLoader.SPLIT_NONE,
      0,
      "cp037",
      Convert.FMT_MAINFRAME,
      0,
      null);
  }

  public static int getRecordLength(ExternalRecord externalRecord,
                                    int fileStructure) {
    int recordByteLength = 0;
    for (ExternalField field : externalRecord.getRecordFields()) {
      recordByteLength += field.getLen();
    }
    if (Constants.IO_VB == fileStructure) {
      recordByteLength += 4;
    }
    return recordByteLength;
  }

  public static AbstractLineReader getAndOpenLineReader(InputStream is,
                                                        int fileStructure,
                                                        ExternalRecord externalRecord) throws RecordException, IOException {
    LineProvider lineProvider = LineIOProvider.getInstance()
      .getLineProvider(fileStructure, "cp037");
    AbstractLineReader reader = LineIOProvider.getInstance()
      .getLineReader(fileStructure, lineProvider);
    LayoutDetail copyBook = ToLayoutDetail.getInstance()
      .getLayout(externalRecord);

    reader.open(is, copyBook);
    return reader;
  }

  public static String parseFieldDelimiter(String fieldDelimiter) {
    Preconditions.checkArgument(!fieldDelimiter.isEmpty(),
      "Cannot specify an empty field delimiter");

    if (fieldDelimiter.startsWith("0x")) {
      int codePoint = Integer.valueOf(fieldDelimiter.substring(2), 16);
      return new Character((char) codePoint).toString();
    } else {
      return fieldDelimiter;
    }
  }

}
