package com.zozospider.hadoop.mapreduce.compress.reduceout;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper
 */
public class ReduceOutMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // key out
    private Text keyOut = new Text();
    // value out
    private static final IntWritable valueOut = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: abc abc love

        // 1 获取 1 行
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" ");

        // 3 循环写出
        for (String word : words) {

            keyOut.set(word);

            // key_out: abc
            // value_out: 1

            // Map 写出
            context.write(keyOut, valueOut);
        }
    }

}
