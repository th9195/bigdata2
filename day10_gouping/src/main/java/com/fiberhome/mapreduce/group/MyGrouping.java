package com.fiberhome.mapreduce.group;

import com.fiberhome.mapreduce.bean.OrderBean;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGrouping extends WritableComparator {

    // 调用父类有参构造
    public MyGrouping() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean order1 = (OrderBean) a;
        OrderBean order2 = (OrderBean) b;

        // 这个方法只看什么时候等于0 。 如果两个orderId 相等。 就放在同一组
        return order1.getOrderId().compareTo(order2.getOrderId());
    }
}
