package com.zozospider.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper
 */
public class GroupingComparatorMapper extends Mapper<LongWritable, Text, GroupingComparatorKeyWritable, Text> {

    private GroupingComparatorKeyWritable keyOut = new GroupingComparatorKeyWritable();
    private Text valueOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key_in: 0
        // value_in: one 1 100

        // 1 获取 1 行
        String line = value.toString();

        // 2 切割
        String[] fields = line.split(" ");
        keyOut.setField1(Integer.valueOf(fields[1]));
        keyOut.setField2(Integer.valueOf(fields[2]));
        valueOut.set(fields[0]);

        // key_out: GroupingComparatorKeyWritable{field1=1, field2=100}
        // value_out: one

        // 4 Map 写出
        context.write(keyOut, valueOut);
    }

}
