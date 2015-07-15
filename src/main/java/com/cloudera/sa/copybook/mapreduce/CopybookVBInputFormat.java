package com.cloudera.sa.copybook.mapreduce;

import com.cloudera.sa.copybook.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class CopybookVBInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
                                                             TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new CopybookVBRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  public static void setCopybookHdfsPath(Configuration config, String value) {
    config.set(Constants.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF, value);
  }

}
