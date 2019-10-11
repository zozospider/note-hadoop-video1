package com.zozospider.hadoop.mapreduce.comparable.all;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class AllReducer extends Reducer<AllKeyWritable, Text, AllKeyWritable, Text> {

    @Override
    protected void reduce(AllKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key_in: AllKeyWritable{field1=xx, field2=xx, fieldSum=25}
        // value_in: [abc, love]

        for (Text value : values) {
            context.write(key, value);
        }
    }

}
