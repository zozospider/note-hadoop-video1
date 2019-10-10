package com.zozospider.hadoop.mapreduce.input.keyvaluetext;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map 阶段
 */
public class KeyValueTextMapper extends Mapper<Text, Text, Text, IntWritable> {

    // value out
    private static final IntWritable valueOut = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // org_text: abc abc love
        // key_in: abc
        // value_in: abc love

        // 写出
        context.write(key, valueOut);
    }

}
