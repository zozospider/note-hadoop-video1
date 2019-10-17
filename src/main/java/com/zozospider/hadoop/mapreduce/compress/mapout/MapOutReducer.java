package com.zozospider.hadoop.mapreduce.compress.mapout;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class MapOutReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable valueOut = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // key_in: abc
        // values_in: [1, 1]

        int sum = 0;

        // 1 累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        valueOut.set(sum);

        // key_out: abc
        // value_out: 2

        // 2 Reduce 写出
        context.write(key, valueOut);
    }

}
