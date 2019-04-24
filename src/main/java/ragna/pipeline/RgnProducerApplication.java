package ragna.pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages="ragna.pipeline")
public class RgnProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(RgnProducerApplication.class, args);
	}

}
