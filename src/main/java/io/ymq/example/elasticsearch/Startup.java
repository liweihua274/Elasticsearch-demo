package io.ymq.example.elasticsearch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 *
 */
@SpringBootApplication
@ComponentScan(value = {"io.ymq.example.elasticsearch"})
@EnableAutoConfiguration
public class Startup {




	public static void main(String[] args) {
		SpringApplication.run(Startup.class, args);
	}






}
