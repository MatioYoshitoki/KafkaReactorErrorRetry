package com.test.kafka.pojo;

import cn.hutool.core.date.DateUtil;

import java.io.Serializable;
import java.util.Date;

public class TestKafka implements Serializable {

    private String name;
    private Integer age;
    private String school;
    private Long processTime;

    public TestKafka() {
    }

    public TestKafka(String name, Integer age, String school, Long processTime) {
        this.name = name;
        this.age = age;
        this.school = school;
        this.processTime = processTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getSchool() {
        return school;
    }

    public void setSchool(String school) {
        this.school = school;
    }

    public Long getProcessTime() {
        return processTime;
    }

    public void setProcessTime(Long processTime) {
        this.processTime = processTime;
    }
}
