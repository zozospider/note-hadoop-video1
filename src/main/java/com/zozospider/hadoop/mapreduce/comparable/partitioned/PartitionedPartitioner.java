package com.zozospider.hadoop.mapreduce.comparable.partitioned;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义 Partitioner: 按每 1 个 value 的首字母分区 (a-k, l-z)
 */
public class PartitionedPartitioner extends Partitioner<PartitionedKeyWritable, Text> {

    @Override
    public int getPartition(PartitionedKeyWritable partitionedKeyWritable, Text text, int numPartitions) {

        String line = text.toString();
        char firstChar = line.charAt(0);

        if (firstChar <= 'k') {
            return 0;
        }
        return 1;
    }

}
