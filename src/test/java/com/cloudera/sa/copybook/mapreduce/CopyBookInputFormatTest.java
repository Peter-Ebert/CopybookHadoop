package com.cloudera.sa.copybook.mapreduce;

import com.cloudera.sa.copybook.Const;
import net.sf.JRecord.Common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CopyBookInputFormatTest {

  public static RecordReader getRecordReader(String cobolLocation,
                                             String datafileLocation,
                                             String delimiter,
                                             int fileFormat) throws IOException, InterruptedException {
    Configuration conf = new Configuration(false);
    conf.set("fs.default.name", "file:///");
    conf.set(Const.COPYBOOK_INPUTFORMAT_CBL_HDFS_PATH_CONF, cobolLocation);
    conf.set(Const.COPYBOOK_INPUTFORMAT_OUTPUT_DELIMITER, "\t");

    File testFile = new File(datafileLocation);
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat inputFormat = ReflectionUtils
        .newInstance(CopybookInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf,
        new TaskAttemptID());

    RecordReader reader = inputFormat.createRecordReader(split, context);
    reader.initialize(split, context);

    return reader;
  }

  @Test
  public void testFixedRecordReader() throws IOException, InterruptedException {
    RecordReader reader = getRecordReader("src/test/resources/DTAR020.cbl",
        "src/test/resources/DTAR020.bin", "0x01", Constants.IO_FIXED_LENGTH);

    int counter = 0;
    while (reader.nextKeyValue()) {
      counter++;
      System.out.println(reader.getCurrentKey() + ":: " + reader.getCurrentValue());
    }
    assertEquals(379, counter);
  }

}
