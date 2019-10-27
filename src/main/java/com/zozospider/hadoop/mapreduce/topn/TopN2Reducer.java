package com.zozospider.hadoop.mapreduce.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Reducer
 */
public class TopN2Reducer extends Reducer<TopN2KeyWritable, Text, TopN2KeyWritable, Text> {

    private TreeMap<TopN2KeyWritable, org.apache.hadoop.io.Text> map = new TreeMap<>();

    @Override
    protected void reduce(TopN2KeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key_in: TopN1KeyWritable{field1=xx, field2=xx, fieldSum=25}
        // value_in: [abc, love]

        for (Text value : values) {

            TopN2KeyWritable k = new TopN2KeyWritable();
            k.set(key.getField1(), key.getField2());
            Text v = new Text(value);

            // k: TopN1KeyWritable{field1=10, field2=15, fieldSum=25}
            // v: abc
            // k: TopN1KeyWritable{field1=15, field2=10, fieldSum=25}
            // v: love

            if (map.size() <= 8) {
                map.put(k, v);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // 写出
        Set<Map.Entry<TopN2KeyWritable, Text>> entries = map.entrySet();
        for (Map.Entry<TopN2KeyWritable, Text> entry : entries) {
            context.write(entry.getKey(), entry.getValue());
        }
    }

}
