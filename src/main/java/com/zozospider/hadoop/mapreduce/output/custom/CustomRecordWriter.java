package com.zozospider.hadoop.mapreduce.output.custom;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CustomRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream outputAToK;
    private FSDataOutputStream outputLToZ;

    public CustomRecordWriter(TaskAttemptContext job) {

        try {
            // 获取文件系统
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());

            // 获取输出路径
            String outDir = job.getConfiguration().get(FileOutputFormat.OUTDIR);

            // 创建输出流 AToK.log 和 LToZ.log
            outputAToK = fileSystem.create(new Path(outDir + "/AToK.log"));
            outputLToZ = fileSystem.create(new Path(outDir + "/LToZ.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 按每 1 个 key 的首字母输出到不同文件 (a-k, l-z)

        String line = key.toString();
        char firstChar = line.charAt(0);

        if (firstChar <= 'k') {
            outputAToK.write(key.getBytes());
        } else {
            outputLToZ.write(key.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(outputAToK);
        IOUtils.closeStream(outputLToZ);
    }

}
