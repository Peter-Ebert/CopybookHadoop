package com.cloudera.sa.copybook.mapreduce;

import com.cloudera.sa.copybook.common.Constants;
import com.cloudera.sa.copybook.common.CopybookIOUtils;
import com.google.common.base.Preconditions;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.IO.AbstractLineReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;

public class CopybookRecordReader extends RecordReader<LongWritable, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(CopybookRecordReader.class);

  private int recordByteLength;
  private long start;
  private long pos;
  private long end;

  private LongWritable key = null;
  private Text value = null;

  AbstractLineReader ret;
  ExternalRecord externalRecord;
  String fieldDelimiter;
  int fileStructure;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {

    // Get configuration
    String cblPath = context.getConfiguration().get(
      Constants.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF);
    LOG.info("Cobol Path: {}", cblPath);

    fieldDelimiter = CopybookIOUtils.parseFieldDelimiter(
      context.getConfiguration().get(
        Constants.COPYBOOK_INPUTFORMAT_FIELD_DELIMITER,
        Constants.DEFAULT_OUTPUT_DELIMITER));
    LOG.info("Output Delimiter: {}", fieldDelimiter);

    fileStructure = context.getConfiguration().getInt(
      Constants.COPYBOOK_INPUTFORMAT_FILE_STRUCTURE,
      Constants.DEFAULT_FILE_STRUCTURE);
    Preconditions.checkArgument(
      Constants.SUPPORTED_FILE_STRUCTURES.contains(fileStructure),
      "Supported file structures: " + Constants.SUPPORTED_FILE_STRUCTURES);
    LOG.info("Input File Structure: {}", fileStructure == net.sf.JRecord.Common.Constants.IO_FIXED_LENGTH ? "FB" : "VB");

    // Open InputStream to Cobol layout file on HDFS
    FileSystem fs = FileSystem.get(context.getConfiguration());
    BufferedInputStream inputStream = new BufferedInputStream(fs.open(new Path(
      cblPath)));

    try {
      externalRecord = CopybookIOUtils.getExternalRecord(inputStream);
      recordByteLength = CopybookIOUtils
        .getRecordLength(externalRecord, fileStructure);
      LOG.info("Record length: {}", recordByteLength);

      FileSplit fileSplit = (FileSplit) split;

      start = fileSplit.getStart();
      end = start + fileSplit.getLength();

      BufferedInputStream fileIn = new BufferedInputStream(fs.open(fileSplit
        .getPath()));

      // Jump to the point in the split at which the first
      // whole record of split starts if not the first InputSplit
      if (start != 0) {
        pos = start - (start % recordByteLength) + recordByteLength;
        fileIn.skip(pos);
      }

      ret = CopybookIOUtils.getAndOpenLineReader(fileIn, fileStructure,
        externalRecord);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (pos > end) {
      return false;
    }

    if (key == null) {
      key = new LongWritable();
    }
    if (value == null) {
      value = new Text();
    }

    AbstractLine line = ret.read();

    if (line == null) {
      return false;
    }

    pos += recordByteLength;

    key.set(pos);

    StringBuilder strBuilder = new StringBuilder();

    boolean isFirst = true;
    int i = 0;
    for (ExternalField field : externalRecord.getRecordFields()) {
      if (isFirst) {
        isFirst = false;
      } else {
        strBuilder.append(fieldDelimiter);
      }
      strBuilder.append(line.getFieldValue(0, i++));
    }

    value.set(strBuilder.toString());
    key.set(pos);

    return true;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {

    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  @Override
  public void close() throws IOException {
    ret.close();
  }

}
