package com.cloudera.sa.copybook.mapred;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class CopybookInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
                                                          JobConf job,
                                                          Reporter reporter) throws IOException {
    return new CopybookRecordReader((FileSplit) split, job);
  }
}
