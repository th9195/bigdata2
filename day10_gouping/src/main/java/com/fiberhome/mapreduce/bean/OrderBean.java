package com.fiberhome.mapreduce.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double money;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }


    @Override
    public String toString() {
        return "OrderBean{" +
                "orderId='" + orderId + '\'' +
                ", money=" + money +
                '}';
    }

    @Override
    public int compareTo(OrderBean o) {

        // 注意：1- 根据什么分组，就必须先要根据这个排序；2- 取topN 也要根据这个排序；
        int result = this.orderId.compareTo(o.orderId);
        if ( 0 == result ) {
            result = (int) (o.money - this.money );
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeDouble(money);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.money = dataInput.readDouble();
    }
}
