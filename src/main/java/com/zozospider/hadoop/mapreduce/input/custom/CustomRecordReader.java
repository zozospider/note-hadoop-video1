package com.zozospider.hadoop.mapreduce.input.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CustomRecordReader extends RecordReader<Text, BytesWritable> {

    FileSplit fileSplit;
    Configuration configuration;
    Text keyIn = new Text();
    BytesWritable valueIn = new BytesWritable();
    boolean isProgress = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        this.fileSplit = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (isProgress) {
            byte[] buf = new byte[(int) fileSplit.getLength()];

            // 1 获取 fs 对象
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(configuration);

            // 2 获取输入流
            FSDataInputStream input = fs.open(path);

            // 3 拷贝
            IOUtils.readFully(input, buf, 0, buf.length);

            // 4 封装
            valueIn.set(buf, 0, buf.length);
            keyIn.set(path.toString());

            // 5 关闭资源
            IOUtils.closeStream(input);

            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return keyIn;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return valueIn;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

}
