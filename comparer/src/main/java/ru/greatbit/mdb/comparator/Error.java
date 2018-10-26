package ru.greatbit.mdb.comparator;

public class Error {
    private final String message;
    private String path1;
    private String path2;

    public Error(String message) {
        this.message = message;
    }

    public Error(String message, String path1, String path2) {
        this.message = message;
        this.path1 = path1;
        this.path2 = path2;
    }

    public String getMessage() {
        return message;
    }

    public String getPath1() {
        return path1;
    }

    public String getPath2() {
        return path2;
    }

    public void setPath1(String path1) {
        this.path1 = path1;
    }

    public void setPath2(String path2) {
        this.path2 = path2;
    }

    @Override
    public String toString() {
        return "Error{" +
                "message='" + message + '\'' +
                ", path1='" + path1 + '\'' +
                ", path2='" + path2 + '\'' +
                '}';
    }
}
