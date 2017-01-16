package com.fonuder.model;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by tiger on 2017/1/12.
 */
public class TestObj implements Serializable{

    private static final long serialVersionUID = 1L;

    private int _id;
    private String name;
    private int age;
    private Date birthdate;

    public TestObj(int _id, String name, int age, Date birthdate) {
        this._id = _id;
        this.name = name;
        this.age = age;
        this.birthdate = birthdate;
    }

    public int get_id() {
        return _id;
    }

    public void set_id(int _id) {
        this._id = _id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Date getBirthdate() {
        return birthdate;
    }

    public void setBirthdate(Date birthdate) {
        this.birthdate = birthdate;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
