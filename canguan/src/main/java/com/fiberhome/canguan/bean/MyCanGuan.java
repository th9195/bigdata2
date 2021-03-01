package com.fiberhome.canguan.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyCanGuan implements WritableComparable<MyCanGuan> {

    private  String name ;
    private int income;
    private int  expenditure ;
    private  String  year;
    private int inOrout;
    private String inOroutStr;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIncome() {
        return income;
    }

    public void setIncome(int income) {
        this.income = income;
    }

    public int getExpenditure() {
        return expenditure;
    }

    public void setExpenditure(int expenditure) {
        this.expenditure = expenditure;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public int getInOrout() {
        return inOrout;
    }

    public void setInOrout(int inOrout) {
        this.inOrout = inOrout;
    }

    public String getInOroutStr() {
        return inOroutStr;
    }

    public void setInOroutStr(String inOroutStr) {
        this.inOroutStr = inOroutStr;
    }


    // 需求四  '\t'分割
    @Override
    public String toString() {
        return  name + '\t' +
                income + "\t" +
                expenditure + "\t" +
                year + '\t' +
                inOrout + '\t' +
                inOroutStr + '\t' ;
    }

    @Override
    public int compareTo(MyCanGuan o) {

        // 需求三对每一年的营业安装净盈利排序
        return this.inOrout - o.getInOrout();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
