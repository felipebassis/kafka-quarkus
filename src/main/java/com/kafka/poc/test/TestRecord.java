package com.kafka.poc.test;

import lombok.Data;

import java.io.Serializable;

@Data
public class TestRecord implements Serializable {
    private static final long serialVersionUID = 548516171088213369L;

    private String message;
}
