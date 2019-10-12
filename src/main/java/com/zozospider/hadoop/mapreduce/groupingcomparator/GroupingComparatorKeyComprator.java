package com.zozospider.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * GroupingComparator 分组阶段: 实现 WritableComparator 接口
 */
public class GroupingComparatorKeyComprator extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 用于 key 排序 (return 1 / -1) 和分组 (return 0)
        // 在此使用 key.field1 作为排序分组判断标准, 升序排列.

        GroupingComparatorKeyWritable aKey = (GroupingComparatorKeyWritable) a;
        GroupingComparatorKeyWritable bKey = (GroupingComparatorKeyWritable) b;

        /*
        // 与下面等价
        if (aKey.getField1() > bKey.getField1()) {
            return 1;
        } else if (aKey.getField1() < bKey.getField1()) {
            return -1;
        } else {
            return 0;
        }
        */
        return Integer.compare(aKey.getField1(), bKey.getField1());
    }

}
