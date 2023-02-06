package org.example;


public class UserModel {

    private String id;
    private String fname;
    private String lname;
    private String email;
    private String password;
    public UserModel(String id,String fname,String lname,String email,String password){
        this.id = id;
        this.fname = fname;
        this.lname=lname;
        this.email = email;
        this.password=password;
    }
    public String getId(){
        return id;
    }
    public String getFname(){
        return fname;
    }
    public String getLname(){
        return lname;
    }
    public String getEmail(){
        return email;
    }
    public String getPassword(){
        return password;
    }
    public void setId(String id){
        this.id=id;
    }
    public void setFname(String fname){
        this.fname=fname;
    }
    public void setLname(String lname){
        this.lname=lname;
    }
    public void setEmail(String email){
        this.email=email;
    }
    public void  setPassword(String password){
        this.password=password;
    }
}
