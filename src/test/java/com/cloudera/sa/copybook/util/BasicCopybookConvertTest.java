package com.cloudera.sa.copybook.util;

import static org.junit.Assert.fail;
import net.sf.JRecord.Common.Constants;

import org.junit.Before;
import org.junit.Test;

public class BasicCopybookConvertTest {

	private String cblFile = "src/test/resources/sample.cbl";
	private String cblData = "src/test/resources/sample.dat";
	
	private BasicCopybookConvert converter;

	@Before
	public void setup() {
	  converter = new BasicCopybookConvert().fileStructure(Constants.IO_VB);
	}

	@Test
	public void testConvertSample() {
	  try {
		converter.convertCopybookData(cblFile, cblData, null);
      } catch (Exception e) {
		e.printStackTrace();
		fail();
      }
	}

}
