package com.epam.twitter_blacklist.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SuspiciousActivity {
    private Integer userId;
    private String userFullName;
    private String userAddress;
    private String message;
    private String category;
    private String messageDate;
}
