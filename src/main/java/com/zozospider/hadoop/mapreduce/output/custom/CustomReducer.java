package com.zozospider.hadoop.mapreduce.output.custom;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class CustomReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    Text keyOut = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // key_in: abc do the best
        // value_in: NULL

        keyOut.set(key.toString() + "\r\n");

        // 循环写出, 由 CustomOutputFormat 决定当前 keyOut 写出到哪个文件
        for (NullWritable value : values) {
            context.write(keyOut, NullWritable.get());
        }
    }

}
