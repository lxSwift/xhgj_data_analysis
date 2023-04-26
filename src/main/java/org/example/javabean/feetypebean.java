package org.example.javabean;

/**
 * @Author luoxin
 * @Date 2023/4/25 16:46
 * @PackageName:org.example.javabean
 * @ClassName: feeType
 * @Description: TODO
 * @Version 1.0
 */
public class feetypebean {
    private String id;
    private String name;
    private String parentId;
    private String active;
    private String code;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getActive() {
        return active;
    }

    public void setActive(String active) {
        this.active = active;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public feetypebean(String id, String name, String parentId, String active, String code) {
        this.id = id;
        this.name = name;
        this.parentId = parentId;
        this.active = active;
        this.code = code;
    }

    public feetypebean(){

    }
    @Override
    public String toString() {
        return "feeType{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", parentId='" + parentId + '\'' +
                ", active='" + active + '\'' +
                ", code='" + code + '\'' +
                '}';
    }
}
