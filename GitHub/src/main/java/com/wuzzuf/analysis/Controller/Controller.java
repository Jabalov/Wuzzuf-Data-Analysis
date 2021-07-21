package com.wuzzuf.analysis.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller
{
    @RequestMapping("/greeting")
    public String getGreetings()
    {
        return "Hello World!";
    }
}
