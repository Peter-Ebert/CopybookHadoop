package com.cloudera.sa.copybook.mapreduce;

import com.cloudera.sa.copybook.Const;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class CopybookInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
                                                             TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new CopybookRecordReader();
  }

  public static void setCopybookHdfsPath(Configuration config, String value) {
    config.set(Const.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF, value);
  }

}
