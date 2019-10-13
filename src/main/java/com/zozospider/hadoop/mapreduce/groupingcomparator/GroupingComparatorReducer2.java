package com.zozospider.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduce 阶段
 */
public class GroupingComparatorReducer2 extends Reducer<GroupingComparatorKeyWritable, Text, GroupingComparatorKeyWritable, Text> {

    @Override
    protected void reduce(GroupingComparatorKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key_in: GroupingComparatorKeyWritable{field1=1, field2=300}
        // value_in: [one., one.., one]

        // 写出 2 行

        // keyOut: GroupingComparatorKeyWritable{field1=1, field2=300}
        // valueOut: values: one.

        // keyOut: GroupingComparatorKeyWritable{field1=1, field2=200}
        // valueOut: values: one..

        int i = 0;
        for (Text value : values) {
            if (i < 2) {
                i++;
                context.write(key, value);
            } else {
                break;
            }
        }
    }

}
