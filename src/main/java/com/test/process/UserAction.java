package com.test.process;

import java.io.Serializable;

public class UserAction implements Serializable {
    private int userId;
    private String page;
    private String action;
    private String userActionTime;

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getUserActionTime() {
        return userActionTime;
    }

    public void setUserActionTime(String userActionTime) {
        this.userActionTime = userActionTime;
    }

    @Override
    public String toString() {
        return "UserAction{" +
                "userId=" + userId +
                ", page='" + page + '\'' +
                ", action='" + action + '\'' +
                ", userActionTime='" + userActionTime + '\'' +
                '}';
    }
}
