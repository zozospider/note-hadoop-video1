package com.zozospider.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现 Writable 接口
 */
public class GroupingComparatorKeyWritable implements WritableComparable<GroupingComparatorKeyWritable> {

    private int field1;
    private int field2;

    public GroupingComparatorKeyWritable() {
        super();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(field1);
        out.writeInt(field2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.field1 = in.readInt();
        this.field2 = in.readInt();
    }

    @Override
    public String toString() {
        return "GroupingComparatorKeyWritable{" +
                "field1=" + field1 +
                ", field2=" + field2 +
                '}';
    }

    @Override
    public int compareTo(GroupingComparatorKeyWritable o) {
        // 按 field1 升序排列, 相同再按 field2 降序排序

        if (field1 > o.getField1()) {
            return 1;
        } else if (field1 < o.getField1()) {
            return -1;
        } else {

            // 与下面等价
            /*
            if (field2 > o.getField2()) {
                return -1;
            } else if (field2 < o.getField2()) {
                return 1;
            } else {
                return 0;
            }
            */
            return Integer.compare(o.getField2(), field2);
        }
    }

    public int getField1() {
        return field1;
    }

    public void setField1(int field1) {
        this.field1 = field1;
    }

    public int getField2() {
        return field2;
    }

    public void setField2(int field2) {
        this.field2 = field2;
    }

}
