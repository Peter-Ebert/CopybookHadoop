package com.cloudera.sa.copybook.util;

import com.cloudera.sa.copybook.common.CopybookIOUtils;
import net.sf.JRecord.Common.FieldDetail;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.IO.AbstractLineReader;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import static org.junit.Assert.*;

public class CopybookIOUtilsTest {

  @Test
  public void testParseCbl() throws FileNotFoundException {
    InputStream testIs = new FileInputStream("src/test/resources/E0006684.TXT");
    try {
      ExternalRecord rec = CopybookIOUtils.getExternalRecord(testIs);
      rec.setDelimiter("");
      assertEquals(28, rec.getNumberOfRecordFields());
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testReadCblVarcharDataFile() throws FileNotFoundException {
    InputStream cblIs = new FileInputStream("src/test/resources/E0006684.TXT");
    InputStream cblDataIs = new FileInputStream("src/test/resources/E0006684_20150819");
    try {
      ExternalRecord rec = CopybookIOUtils.getExternalRecord(cblIs);
      assertEquals(28, rec.getNumberOfRecordFields());
      AbstractLineReader reader = CopybookIOUtils.getAndOpenLineReader(cblDataIs, net.sf.JRecord.Common.Constants.IO_VB, rec);

      AbstractLine line = null;
      int numRead = 0;
      while ((line = reader.read()) != null && numRead < 10) {
        String out = "";
        String delim = ",";
//        String currentGroup = "";
        boolean inPotentialVarcharGroup = false;
        boolean inVarcharGroup = false;
        int currentRealPos = 1;
        int varCharLen = 0;
        int i = 0;
        int numFields = rec.getRecordFields().length;
        for (ExternalField field : rec.getRecordFields()) {
          FieldDetail detail = line.getLayout().getField(0, i);
          if (field.getName().endsWith("-LEN")) {
            inPotentialVarcharGroup = true;
            detail.setPosLen(currentRealPos, field.getLen());
            varCharLen = line.getFieldValue(detail).asInt();
          } else if (inPotentialVarcharGroup && field.getName().endsWith("-TEXT")) {
            inVarcharGroup = true;
            inPotentialVarcharGroup = false;
          } else {
            // Wasn't a varchar
            inPotentialVarcharGroup = false;
          }

          if (inVarcharGroup) {
            detail.setPosLen(currentRealPos, varCharLen);
            currentRealPos += varCharLen;
            inVarcharGroup = false;
          } else {
            detail.setPosLen(currentRealPos, field.getLen());
            currentRealPos += field.getLen();
          }
          out += line.getFieldValue(detail).asString();
          if (i < numFields - 1) {
            out += delim;
          }
          i++;
        }
        System.out.println(out);
        assertNotNull(line);
        numRead++;
      }
    } catch (Exception e) {
      fail();
    }
  }

}
