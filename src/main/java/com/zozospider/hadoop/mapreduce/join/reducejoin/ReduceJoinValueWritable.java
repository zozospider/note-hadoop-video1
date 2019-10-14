package com.zozospider.hadoop.mapreduce.join.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现 Writable 接口
 */
public class ReduceJoinValueWritable implements Writable {

    private String flag;
    private int aId;
    private String aName;
    private int bId;
    private String bName;

    public ReduceJoinValueWritable() {
        super();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(flag);
        out.writeInt(aId);
        out.writeUTF(aName);
        out.writeInt(bId);
        out.writeUTF(bName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        flag = in.readUTF();
        aId = in.readInt();
        aName = in.readUTF();
        bId = in.readInt();
        bName = in.readUTF();
    }

    @Override
    public String toString() {
        return "ReduceJoinValueWritable{" +
                "flag='" + flag + '\'' +
                ", aId=" + aId +
                ", aName='" + aName + '\'' +
                ", bId=" + bId +
                ", bName='" + bName + '\'' +
                '}';
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public int getaId() {
        return aId;
    }

    public void setaId(int aId) {
        this.aId = aId;
    }

    public String getaName() {
        return aName;
    }

    public void setaName(String aName) {
        this.aName = aName;
    }

    public int getbId() {
        return bId;
    }

    public void setbId(int bId) {
        this.bId = bId;
    }

    public String getbName() {
        return bName;
    }

    public void setbName(String bName) {
        this.bName = bName;
    }

}
