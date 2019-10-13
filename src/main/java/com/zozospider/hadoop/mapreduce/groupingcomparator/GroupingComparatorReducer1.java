package com.zozospider.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduce 阶段
 */
public class GroupingComparatorReducer1 extends Reducer<GroupingComparatorKeyWritable, Text, GroupingComparatorKeyWritable, Text> {

    Text valueOut = new Text();

    @Override
    protected void reduce(GroupingComparatorKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key_in: GroupingComparatorKeyWritable{field1=1, field2=300}
        // value_in: [one., one.., one]

        // 如果迭代 values, 会每次更新对应的 key 值 (参考 java.lang.Iterable 的实现: org.apache.hadoop.mapreduce.task.ReduceContextImpl.ValueIterable)
        /*System.out.println("key0: " + key);

        for (Text value : values) {
            System.out.println("key: " + key + ", value: " + value);
            context.write(key, value);
        }*/


        valueOut.set(values.iterator().next());

        // keyOut: GroupingComparatorKeyWritable{field1=1, field2=300}
        // valueOut: values: one.

        // 写出
        context.write(key, valueOut);
    }

}
