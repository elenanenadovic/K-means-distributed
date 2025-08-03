package org.example;

//i made this one without color so gson can be used, same as location

public class Location2{
    public String name;
    public double capacity;
    public double la;
    public double lo;


    public Location2(String name,double capacity,double latitude,double longitude){
        this.name=name;
        this.capacity=capacity;
        this.la=latitude;
        this.lo=longitude;
    }
}
