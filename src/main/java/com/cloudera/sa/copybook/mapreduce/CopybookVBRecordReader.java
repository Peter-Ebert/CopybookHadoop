package com.cloudera.sa.copybook.mapreduce;

import com.cloudera.sa.copybook.common.Constants;
import com.cloudera.sa.copybook.common.CopybookIOUtils;
import com.google.common.base.Preconditions;
import net.sf.JRecord.Common.FieldDetail;
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

import java.io.BufferedInputStream;
import java.io.IOException;

public class CopybookVBRecordReader extends RecordReader<LongWritable, Text> {

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

    fieldDelimiter = CopybookIOUtils.parseFieldDelimiter(
        context.getConfiguration().get(
            Constants.COPYBOOK_INPUTFORMAT_FIELD_DELIMITER,
            Constants.DEFAULT_OUTPUT_DELIMITER));

    fileStructure = context.getConfiguration().getInt(
        Constants.COPYBOOK_INPUTFORMAT_FILE_STRUCTURE,
        Constants.DEFAULT_FILE_STRUCTURE);
    Preconditions.checkArgument(
      Constants.SUPPORTED_FILE_STRUCTURES.contains(fileStructure),
      "Supported file structures: " + Constants.SUPPORTED_FILE_STRUCTURES);

    // Open InputStream to Cobol layout file on HDFS
    FileSystem fs = FileSystem.get(context.getConfiguration());
    BufferedInputStream inputStream = new BufferedInputStream(fs.open(new Path(
        cblPath)));

    try {
      externalRecord = CopybookIOUtils.getExternalRecord(inputStream);
      
      FileSplit fileSplit = (FileSplit) split;

      start = fileSplit.getStart();
      end = start + fileSplit.getLength();

      BufferedInputStream fileIn = new BufferedInputStream(fs.open(fileSplit
          .getPath()));

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

    pos += line.getData().length;

    StringBuilder strBuilder = new StringBuilder();
    boolean inPotentialVarcharGroup = false;
    boolean inVarcharGroup = false;
    int currentRealPos = 1;
    int varCharLen = 0;
    int i = 0;
    int numFields = externalRecord.getRecordFields().length;
    for (ExternalField field : externalRecord.getRecordFields()) {
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
      strBuilder.append(line.getFieldValue(detail).asString());
      if (i < numFields - 1) {
        strBuilder.append(fieldDelimiter);
      }
      i++;
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
