package com.zozospider.hadoop.mapreduce.multijob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class MultiJob1Reducer extends Reducer<Text, Text, Text, Text> {

    private Text valueOut = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key_in: abc
        // value_in: ["f1,0", "f1,8", "f1,18", "f2,16", "f3,7", "f3,11"]

        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value.toString());
            builder.append("|");
        }
        builder.delete(builder.length() - 1, builder.length());

        valueOut.set(builder.toString());

        // key_out: abc
        // value_out: f1,0|f1,8|f1,18|f2,16|f3,7|f3,11

        // 写出
        context.write(key, valueOut);
    }

}
