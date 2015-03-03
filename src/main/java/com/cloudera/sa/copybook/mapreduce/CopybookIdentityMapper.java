package com.cloudera.sa.copybook.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CopybookIdentityMapper extends Mapper<LongWritable, Text, Text, Text> {

  private final Text empty = new Text();

  @Override
  public void map(LongWritable key, Text value,
                  Mapper.Context context) throws IOException, InterruptedException {
    context.write(value, empty);
  }

}
