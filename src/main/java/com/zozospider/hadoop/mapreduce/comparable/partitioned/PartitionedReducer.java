package com.zozospider.hadoop.mapreduce.comparable.partitioned;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class PartitionedReducer extends Reducer<PartitionedKeyWritable, Text, PartitionedKeyWritable, Text> {

    @Override
    protected void reduce(PartitionedKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key_in: PartitionedKeyWritable{field1=xx, field2=xx, fieldSum=25}
        // value_in: [abc, love]

        for (Text value : values) {
            context.write(key, value);
        }
    }

}
