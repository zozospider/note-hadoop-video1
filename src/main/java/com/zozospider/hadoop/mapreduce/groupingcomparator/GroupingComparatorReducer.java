package com.zozospider.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduce 阶段
 */
public class GroupingComparatorReducer extends Reducer<GroupingComparatorKeyWritable, Text, GroupingComparatorKeyWritable, Text> {

    Text valueOut = new Text();

    @Override
    protected void reduce(GroupingComparatorKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key_in: GroupingComparatorKeyWritable{field1=1, field2=300}
        // value_in: [one., one.., one]

        // 循环拼装 value
        StringBuffer valueBuffer = new StringBuffer();
        valueBuffer.append("values: ");

        for (Text value : values) {
            valueBuffer.append(value.toString() + " ");
        }

        valueOut.set(valueBuffer.toString());

        // keyOut: GroupingComparatorKeyWritable{field1=1, field2=300}
        // valueOut: values: one. one.. one

        // 写出
        context.write(key, valueOut);
    }

}
