package es.accenture.mlgia;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class MlgiaCollectorApplication {

	public static void main(String[] args) {
		SpringApplication.run(MlgiaCollectorApplication.class, args);
	}
}
