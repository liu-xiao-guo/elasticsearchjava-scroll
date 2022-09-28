
public class Twitter {
    private String user;
    private long uid;
    private String province;
    private String message;
    private String country;
    private String city;
    private long age;
    private String address;

    public Twitter() {
    }

    public Twitter(String user, long uid, String province, String message,
                   String country, String city, long age, String address) {
        this.user = user;
        this.uid = uid;
        this.province = province;
        this.message = message;
        this.country = country;
        this.city = city;
        this.age = age;
        this.address = address;
    }

    public String getUser() {
        return user;
    }

    public long getUid() {
        return uid;
    }

    public String getProvince() {
        return province;
    }

    public String getMessage() {
        return message;
    }

    public String getCountry() {
        return country;
    }

    public String getCity() {
        return city;
    }

    public long getAge() {
        return age;
    }

    public String getAddress() {
        return address;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setUid(long uid) {
        this.uid = uid;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
