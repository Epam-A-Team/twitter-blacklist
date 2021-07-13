package com.epam.twitter_blacklist.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class SuspiciousActivity {
    private String userFullName;
    private String userAddress;
    private String message;
    private String category;
    private String messageDate;
}
