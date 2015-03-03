package com.cloudera.sa.copybook.examples;

import com.cloudera.sa.copybook.Const;
import com.cloudera.sa.copybook.mapreduce.CopybookInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ExampleMapreduceDriver extends Configured implements Tool {

  public static class CopybookIdentityMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text empty = new Text();

    @Override
    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {
      context.write(value, empty);
    }

  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.printf("%s <copybookloc> <input> <output>\n",
          this.getClass().getName());
      return -1;
    }

    // General config
    Job job = Job.getInstance(getConf());
    job.setJobName("Example Copybook format");
    job.setJarByClass(ExampleMapreduceDriver.class);
    job.setNumReduceTasks(0);
    job.getConfiguration()
        .set(Const.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF, args[0]);

    // I/O formats
    job.setInputFormatClass(CopybookInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // I/O paths
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    // MR classes and types
    job.setMapperClass(CopybookIdentityMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Run it
    return job.waitForCompletion(true) ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new ExampleMapreduceDriver(), args));
  }
}
