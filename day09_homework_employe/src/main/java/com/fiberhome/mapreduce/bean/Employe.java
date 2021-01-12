package com.fiberhome.mapreduce.bean;

import com.fiberhome.mapreduce.Utils.DateFormatUtils;
import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 字段解释: 员工编号,姓名,岗位名称,员工上司编号,入职日期,工资,提成,部门编号
 * 7369,SMITH,CLERK,7902,17-12月-80,800,0,20
 * 7499,ALLEN,SALESMAN,7698,20-2月-81,1600,300,30
 */
public class Employe implements WritableComparable<Employe> {

    private String  employeId;      // 员工编号
    private String  name;           // 姓名
    private String  statment;       // 岗位名称
    private String  superiorId;     // 员工上司编号
    private Date    entryDay;       // 入职日期
    private Double  salary;         // 工资
    private Double  extraSalary;    // 提成
    private String  statmentId;     // 部门编号

    public Employe() {
    }

    public String getEmployeId() {
        return employeId;
    }

    public void setEmployeId(String employeId) {
        this.employeId = employeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatment() {
        return statment;
    }

    public void setStatment(String statment) {
        this.statment = statment;
    }

    public String getSuperiorId() {
        return superiorId;
    }

    public void setSuperiorId(String superiorId) {
        this.superiorId = superiorId;
    }

    public Date getEntryDay() {
        return entryDay;
    }

    public void setEntryDay(Date entryDay) {
        this.entryDay = entryDay;
    }

    public Double getSalary() {
        return salary;
    }

    public void setSalary(Double salary) {
        this.salary = salary;
    }

    public Double getExtraSalary() {
        return extraSalary;
    }

    public void setExtraSalary(Double extraSalary) {
        this.extraSalary = extraSalary;
    }

    public String getStatmentId() {
        return statmentId;
    }

    public void setStatmentId(String statmentId) {
        this.statmentId = statmentId;
    }


    // 需求4：将源数据中的入职日期：23-1月-82 转为 1982-01-23  注意年份范围限定在(1970-2020年)
    @Override
    public String toString() {
        return employeId + "," +
                name + "," +
                statment + "," +
                superiorId + "," +
                DateFormatUtils.dateToStr(entryDay,"yyyy-MM-dd") + "," +
                salary + "," +
                extraSalary +"," +
                statmentId + ","  ;
    }

    /**
     * 需求3: 将分类后的每个员工的薪资(基本工资 + 提成)进行降序排序
     * @param employe
     * @return
     */
    @Override
    public int compareTo(Employe employe) {
        double thisSalarySum = this.salary + this.extraSalary;
        double otherSalarySum = employe.salary + employe.extraSalary;
        return (int) (otherSalarySum - thisSalarySum);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        /**
         *     private String  employeId;      // 员工编号
         *     private String  name;           // 姓名
         *     private String  statment;       // 岗位名称
         *     private String  superiorId;     // 员工上司编号
         *     private Date    entryDay;       // 入职日期
         *     private Double  salary;         // 工资
         *     private Double  extraSalary;    // 提成
         *     private String  statmentId;     // 部门编号
         */
        dataOutput.writeUTF(employeId);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(statment);
        dataOutput.writeUTF(superiorId);
        dataOutput.writeUTF(DateFormatUtils.dateToStr(entryDay,"yyyy-MM-dd"));
        dataOutput.writeDouble(salary);
        dataOutput.writeDouble(extraSalary);
        dataOutput.writeUTF(statmentId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        /**
         *     private String  employeId;      // 员工编号
         *     private String  name;           // 姓名
         *     private String  statment;       // 岗位名称
         *     private String  superiorId;     // 员工上司编号
         *     private Date    entryDay;       // 入职日期
         *     private Double  salary;         // 工资
         *     private Double  extraSalary;    // 提成
         *     private String  statmentId;     // 部门编号
         */
        employeId = dataInput.readUTF();
        name = dataInput.readUTF();
        statment = dataInput.readUTF();
        superiorId = dataInput.readUTF();
        entryDay = DateFormatUtils.strToDate("yyyy-MM-dd",dataInput.readUTF());
        salary = dataInput.readDouble();
        extraSalary = dataInput.readDouble();
        statmentId = dataInput.readUTF();

    }
}
