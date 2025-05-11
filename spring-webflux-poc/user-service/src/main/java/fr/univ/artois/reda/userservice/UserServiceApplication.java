package fr.univ.artois.reda.userservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

@SpringBootApplication
public class UserServiceApplication {
    public static void main(String[] args) {
        BlockHound.install();
        SpringApplication.run(UserServiceApplication.class, args);
    }
}
