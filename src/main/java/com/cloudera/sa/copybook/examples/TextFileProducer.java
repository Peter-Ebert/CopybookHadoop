package com.cloudera.sa.copybook.examples;

import com.cloudera.sa.copybook.common.Constants;
import com.cloudera.sa.copybook.mapreduce.CopybookInputFormat;
import com.cloudera.sa.copybook.mapreduce.CopybookVBInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TextFileProducer extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(TextFileProducer.class);

  private enum Type {
    FB,
    VB,
    VBV
  }

  public static class CopybookSubstitutionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private final NullWritable empty = NullWritable.get();
    private final Text newValue = new Text();
    private Map<String, String> subsMap = new HashMap<String, String>();
    private boolean hasSubs = false;

    @Override
    protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
      throws IOException, InterruptedException {
      String subsStr = context.getConfiguration().get("char.subs");
      if (null != subsStr && !subsStr.isEmpty()) {
        hasSubs = true;
        String[] fields = subsStr.split(",");
        for (String s : fields) {
          String[] bits = s.split(":");
          subsMap.put(bits[0], bits[1]);
          LOG.info("Substituting [{}] with [{}]", bits[0], bits[1]);
        }
      }
    }

    @Override
    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {
      if (hasSubs) {
        String valStr = value.toString();
        for (Entry<String, String> e : subsMap.entrySet()) {
          valStr = valStr.replaceAll(e.getKey(), e.getValue());
        }
        newValue.set(valStr);
        context.write(empty, newValue);
      } else {
        context.write(empty, value);
      }
    }

  }

  public int run(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.printf("%s [options] <copybookloc> <input> <output> {fb|vb|vbv}>\n\n%s\n",
        this.getClass().getName(),
        "Options:\n  -D char.subs=c1:r1,c2:r2");
      return -1;
    }

    // Check format
    Type type = Type.valueOf(args[3].toUpperCase());

    // General config
    Job job = Job.getInstance(getConf());
    job.setJobName("JRecord: [" + args[1] + "]");
    job.setJarByClass(TextFileProducer.class);
    job.setNumReduceTasks(0);
    job.getConfiguration().set(Constants.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF, args[0]);
    if (type.equals(Type.FB)) {
      job.getConfiguration().set(Constants.COPYBOOK_INPUTFORMAT_FILE_STRUCTURE,
        Integer.toString(net.sf.JRecord.Common.Constants.IO_FIXED_LENGTH));
    } else if (type.equals(Type.VB) || type.equals(Type.VBV)) {
      job.getConfiguration().set(Constants.COPYBOOK_INPUTFORMAT_FILE_STRUCTURE,
        Integer.toString(net.sf.JRecord.Common.Constants.IO_VB));
    } else {
      System.err.printf("Unknown file format: %s\n", type);
      return -1;
    }

    // I/O formats
    if (type.equals(Type.VBV)) {
      job.setInputFormatClass(CopybookVBInputFormat.class);
    } else {
      job.setInputFormatClass(CopybookInputFormat.class);
    }
    job.setOutputFormatClass(TextOutputFormat.class);

    // I/O paths
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    // MR classes and types
    job.setMapperClass(CopybookSubstitutionMapper.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    // Run it
    return job.waitForCompletion(true) ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new TextFileProducer(), args));
  }
}
