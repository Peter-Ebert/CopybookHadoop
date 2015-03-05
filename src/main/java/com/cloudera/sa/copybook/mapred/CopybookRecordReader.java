package com.cloudera.sa.copybook.mapred;

import com.cloudera.sa.copybook.common.Constants;
import com.cloudera.sa.copybook.common.CopybookIOUtils;
import com.google.common.base.Preconditions;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.IO.AbstractLineReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class CopybookRecordReader implements RecordReader<LongWritable, Text> {

  private int recordByteLength;
  private long start;
  private long pos;
  private long end;

  AbstractLineReader ret;
  ExternalRecord externalRecord;

  String fieldDelimiter;

  public CopybookRecordReader(FileSplit genericSplit, JobConf job)
      throws IOException {
    try {
      // Get configuration
      String cblPath = job.get(
          Constants.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF);

      fieldDelimiter = CopybookIOUtils.parseFieldDelimiter(
          job.get(Constants.COPYBOOK_INPUTFORMAT_FIELD_DELIMITER,
              Constants.DEFAULT_OUTPUT_DELIMITER));

      int fileStructure = job.getInt(
          Constants.COPYBOOK_INPUTFORMAT_FILE_STRUCTURE,
          Constants.DEFAULT_FILE_STRUCTURE);
      Preconditions.checkArgument(
          Constants.SUPPORTED_FILE_STRUCTURES.contains(fileStructure),
          "Supported file structures: " + Constants.SUPPORTED_FILE_STRUCTURES);

      if (cblPath == null) {
        if (job != null) {
          MapWork mrwork = Utilities.getMapWork(job);

          if (mrwork == null) {
            System.out.println(
                "When running a client side hive job you have to set " +
                    Constants.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF +
                    " before executing the query.");
            System.out.println(
                "When running a MR job we can get this from the hive TBLProperties");
          }
          Map<String, PartitionDesc> map = mrwork.getPathToPartitionInfo();

          for (Map.Entry<String, PartitionDesc> pathsAndParts : map
              .entrySet()) {
            Properties props = pathsAndParts.getValue().getProperties();
            cblPath = props.getProperty(
                Constants.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF);
            break;
          }
        }
      }

      // Open InputStream to Cobol layout file on HDFS
      FileSystem fs = FileSystem.get(job);
      BufferedInputStream inputStream = new BufferedInputStream(fs.open(new Path(
          cblPath)));

      externalRecord = CopybookIOUtils.getExternalRecord(inputStream);
      recordByteLength = CopybookIOUtils
          .getRecordLength(externalRecord, fileStructure);

      FileSplit fileSplit = (FileSplit) genericSplit;

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

  public boolean next(LongWritable key, Text value) throws IOException {

    try {
      if (pos >= end) {
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
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  public LongWritable createKey() {
    return new LongWritable();
  }

  public Text createValue() {
    return new Text();
  }

  public long getPos() throws IOException {
    return pos;
  }

  public void close() throws IOException {
    ret.close();
  }

  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

}
