package com.zozospider.hadoop.mapreduce.partition.custom;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义 Partitioner: 按每 1 行的首字母分区 (a-f, g-k, l-q, r-z)
 * Key 类型: Mapper 输出的 key 类型
 * Value 类型: Mapper 输出的 value 类型
 */
public class CustomPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {

        String line = text.toString();
        char firstChar = line.charAt(0);

        if (firstChar <= 'f') {
            return 0;
        } else if (firstChar <= 'l') {
            return 1;
        } else if (firstChar <= 'o') {
            return 2;
        }
        return 3;
    }

}
