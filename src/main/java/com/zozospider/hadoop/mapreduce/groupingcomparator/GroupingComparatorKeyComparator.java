package com.zozospider.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * GroupingComparator 分组阶段: 实现 WritableComparator 接口
 */
public class GroupingComparatorKeyComparator extends WritableComparator {

    protected GroupingComparatorKeyComparator() {
        super(GroupingComparatorKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 用于 key 分组 (只会判断是 return 0 / return 非 0, 不会判断正负大小)
        // 在此使用 key.field1 是否相等作为是否是同 1 组的判断标准.

        GroupingComparatorKeyWritable aKey = (GroupingComparatorKeyWritable) a;
        GroupingComparatorKeyWritable bKey = (GroupingComparatorKeyWritable) b;

        // System.out.println("aKey: " + aKey + ", bKey: " + bKey);

        return aKey.getField1() == bKey.getField1() ? 0 : -99;
    }

}
