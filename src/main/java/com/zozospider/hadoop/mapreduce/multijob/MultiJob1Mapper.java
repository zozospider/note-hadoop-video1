package com.zozospider.hadoop.mapreduce.multijob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Mapper
 */
public class MultiJob1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private String fileName;
    private Text keyOut = new Text();
    private Text valueOut = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 获取文件名称
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: abc zoo abc

        // 获取 1 行
        String line = value.toString();

        // 切割
        String[] words = line.split(" ");

        // 循环写出
        int offset = 0;
        for (int i = 0; i < words.length; i++) {

            if (i != 0) {
                offset = line.indexOf(" ", offset) + 1;
            }

            keyOut.set(words[i]);
            valueOut.set(fileName + "," + (key.get() + offset));

            // key_out: abc
            // value_out: f1,0
            // key_out: zoo
            // value_out: f1,4
            // key_out: abc
            // value_out: f1,8

            // 写出
            context.write(keyOut, valueOut);
        }
    }

}
