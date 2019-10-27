package com.zozospider.hadoop.mapreduce.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class TopN1Reducer extends Reducer<TopN1KeyWritable, Text, TopN1KeyWritable, Text> {

    private int counter = 1;

    @Override
    protected void reduce(TopN1KeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key_in: TopN1KeyWritable{field1=xx, field2=xx, fieldSum=25}
        // value_in: [abc, love]

        if (counter > 8) {
            return;
        }

        for (Text value : values) {
            if (counter > 8) {
                return;
            }
            context.write(key, value);
            counter++;
        }
    }

}
