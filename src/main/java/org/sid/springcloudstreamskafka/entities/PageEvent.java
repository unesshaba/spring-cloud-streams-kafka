package org.sid.springcloudstreamskafka.entities;

import lombok.*;

import java.util.Date;
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder

public class PageEvent {
    private String name;
    private String user;
    private Date date;
    private long duration;
}
