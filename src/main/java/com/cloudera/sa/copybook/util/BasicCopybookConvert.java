package com.cloudera.sa.copybook.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import net.sf.JRecord.Common.CommonBits;
import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.Details.LineProvider;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.External.ToLayoutDetail;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.LineIOProvider;
import net.sf.JRecord.Numeric.Convert;
import net.sf.cb2xml.def.Cb2xmlConstants;

public class BasicCopybookConvert {
	
  private int fileStructure = Constants.IO_FIXED_LENGTH;
  private String codePage = "cp037";
  private String separator = "\t";
  
  public BasicCopybookConvert fileStructure(int fileStructure) {
	  this.fileStructure = fileStructure;
	  return this;
  }
  
  public BasicCopybookConvert codePage(String codePage) {
	  this.codePage = codePage;
	  return this;
  }
  
  public BasicCopybookConvert separator(String separator) {
	  this.separator = separator;
	  return this;
  }

  public void convertCopybookData(String copybookFilename, String dataFilename, String outputFilename) throws Exception {
	  
	  CommonBits.setDefaultCobolTextFormat(Cb2xmlConstants.USE_STANDARD_COLUMNS);
	  
	  LineProvider lineProvider = LineIOProvider.getInstance().getLineProvider(fileStructure, codePage);
	  AbstractLineReader reader = LineIOProvider.getInstance().getLineReader(fileStructure, lineProvider);
	  
	  CopybookLoader copybookLdr = new CobolCopybookLoader();
	  ExternalRecord externalRecord = copybookLdr.loadCopyBook(
		copybookFilename, 
		CopybookLoader.SPLIT_NONE, 
		0, 
		codePage,
	    Convert.FMT_MAINFRAME,
	    0,
	    null
	  );
	  
	  LayoutDetail copyBook = ToLayoutDetail.getInstance().getLayout(externalRecord);

	  reader.open(dataFilename, copyBook);

	  System.out.println(reader.getClass());

	  BufferedWriter writer;
	  if (null == outputFilename) {
		writer = new BufferedWriter(new PrintWriter(System.out));
	  } else {
	    writer = new BufferedWriter(new FileWriter(new File(outputFilename)));
	  }

	  AbstractLine line;
	  while ((line = reader.read()) != null) {
		int len = externalRecord.getRecordFields().length;
        for (int i = 0; i < len; i++) {
	      writer.append(line.getFieldValue(0, i).toString());
	      if (i != len - 1) {
	    	  writer.append(separator);
	      }
	    }
        writer.newLine();
      }
      writer.close();
	  reader.close();
  }
  
  private static String getOptionArg(String[] args, int pos) {
	if (args.length > pos) {
      return args[pos + 1];
	} else {
	  System.err.println("Could not parse argument for option " + args[pos] + ": " + args[pos + 1]);
	  usageAndExit(-1);
	}
	return "";
  }
  
  private static void usageAndExit(int retCode) {
	System.err.println("Usage: BasicCopybookConvert [-t fixed|vb] [-s <separator>] -c <cbl file> -d <data file> [-o <output file>]");
    System.exit(retCode);
  }
	
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      usageAndExit(-1);  
    }
    
    BasicCopybookConvert convert = new BasicCopybookConvert();
    String cobolFile = null;
    String dataFile = null;
    String outputFile = null;
    
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-t")) {
        convert.fileStructure(Integer.parseInt(getOptionArg(args,i)));
      } else if (args[i].equals("-s")) {
    	convert.separator(getOptionArg(args, i));
      } else if (args[i].equals("-c")) {
    	cobolFile = getOptionArg(args, i);
      } else if (args[i].equals("-d")) {
    	dataFile = getOptionArg(args, i);
      } else if (args[i].equals("-o")) {
    	outputFile = getOptionArg(args, i);
      }
    }
    
    if (null == cobolFile || null == dataFile) {
    	usageAndExit(-1);
    }
    
    BasicCopybookConvert converter = new BasicCopybookConvert();
    converter.convertCopybookData(cobolFile, dataFile, outputFile);
  }
}
