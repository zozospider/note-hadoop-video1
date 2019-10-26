package com.zozospider.hadoop.mapreduce.multijob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class MultiJob2Reducer extends Reducer<Text, IntWritable, Text, Text> {

    private Text valueOut = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // key_in: abc:f1
        // value_in: [20, 8, 0]

        StringBuilder builder = new StringBuilder();
        for (IntWritable value : values) {
            builder.append(value);
            builder.append(",");
        }
        builder.delete(builder.length() - 1, builder.length());

        valueOut.set(builder.toString());

        // key_out: abc&f3
        // value_out: 12,8

        // 写出
        context.write(key, valueOut);
    }

}
