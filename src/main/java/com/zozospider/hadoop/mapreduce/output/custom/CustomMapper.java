package com.zozospider.hadoop.mapreduce.output.custom;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper
 */
public class CustomMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: abc do the best

        // key_out: abc do the best
        // value_out: NULL

        // 写出
        context.write(value, NullWritable.get());
    }
}
