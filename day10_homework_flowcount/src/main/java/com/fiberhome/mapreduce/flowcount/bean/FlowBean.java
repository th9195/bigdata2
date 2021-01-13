package com.fiberhome.mapreduce.flowcount.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {

    private Long upPackageNum;
    private Long downPackageNum;
    private Long upPayLoad;
    private Long downPayLoad;

    public FlowBean() {
    }

    public FlowBean(Long upPackageNum, Long downPackageNum, Long upPayLoad, Long downPayLoad) {
        this.upPackageNum = upPackageNum;
        this.downPackageNum = downPackageNum;
        this.upPayLoad = upPayLoad;
        this.downPayLoad = downPayLoad;
    }

    public Long getUpPackageNum() {
        return upPackageNum;
    }

    public void setUpPackageNum(Long upPackageNum) {
        this.upPackageNum = upPackageNum;
    }

    public Long getDownPackageNum() {
        return downPackageNum;
    }

    public void setDownPackageNum(Long downPackageNum) {
        this.downPackageNum = downPackageNum;
    }

    public Long getUpPayLoad() {
        return upPayLoad;
    }

    public void setUpPayLoad(Long upPayLoad) {
        this.upPayLoad = upPayLoad;
    }

    public Long getDownPayLoad() {
        return downPayLoad;
    }

    public void setDownPayLoad(Long downPayLoad) {
        this.downPayLoad = downPayLoad;
    }

    @Override
    public String toString() {
        return  upPackageNum + "\t" +
                downPackageNum + "\t" +
                upPayLoad + "\t" +
                downPayLoad ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upPackageNum);
        dataOutput.writeLong(downPackageNum);
        dataOutput.writeLong(upPayLoad);
        dataOutput.writeLong(downPayLoad);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upPackageNum = dataInput.readLong();
        this.downPackageNum = dataInput.readLong();
        this.upPayLoad = dataInput.readLong();
        this.downPayLoad = dataInput.readLong();
    }
}
