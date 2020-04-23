package com.techpago;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class App implements CommandLineRunner {

    public static void main(String[] args) {
        WebApplicationType webType = WebApplicationType.NONE;

        new SpringApplicationBuilder(App.class).web(webType)
                .registerShutdownHook(true).run(args);
    }

    @Override
    public void run(String... args) throws Exception {

    }
}
