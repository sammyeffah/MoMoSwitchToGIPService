package com.etz.gh.model;

public class SwitchGIPResponse {
    private String error;
    private String message;
    private long tat;

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTat() {
        return tat;
    }

    public void setTat(long tat) {
        this.tat = tat;
    }
}
