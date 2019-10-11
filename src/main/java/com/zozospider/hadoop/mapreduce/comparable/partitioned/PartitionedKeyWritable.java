package com.zozospider.hadoop.mapreduce.comparable.partitioned;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现 Writable 接口
 */
public class PartitionedKeyWritable implements WritableComparable<PartitionedKeyWritable> {

    private int field1;
    private int field2;
    // fieldSum = field1 + field2
    private int fieldSum;

    public PartitionedKeyWritable() {
        super();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(field1);
        out.writeInt(field2);
        out.writeInt(fieldSum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.field1 = in.readInt();
        this.field2 = in.readInt();
        this.fieldSum = in.readInt();
    }

    @Override
    public int compareTo(PartitionedKeyWritable o) {
        // 按 fieldSum 倒叙排列
        if (fieldSum > o.getFieldSum()) {
            return -1;
        } else if (fieldSum < o.getFieldSum()) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "PartitionedKeyWritable{" +
                "field1=" + field1 +
                ", field2=" + field2 +
                ", fieldSum=" + fieldSum +
                '}';
    }

    public void set(int field1, int field2) {
        this.field1 = field1;
        this.field2 = field2;
        fieldSum = field2 + field2;
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

    public int getFieldSum() {
        return fieldSum;
    }

    public void setFieldSum(int fieldSum) {
        this.fieldSum = fieldSum;
    }

}
