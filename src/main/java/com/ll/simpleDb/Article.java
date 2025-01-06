package com.ll.simpleDb;

import java.time.LocalDateTime;

public class Article {
    private Long id;
    private String title;
    private String body;
    private LocalDateTime createdDate;
    private LocalDateTime modifiedDate;
    private Boolean isBlind;

    public Long getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getBody() {
        return body;
    }

    public LocalDateTime getCreatedDate() {
        return createdDate;
    }

    public LocalDateTime getModifiedDate() {
        return modifiedDate;
    }

    public Boolean getIsBlind(){
        return isBlind;
    }

    public Boolean isBlind() {
        return isBlind;
    }
}
