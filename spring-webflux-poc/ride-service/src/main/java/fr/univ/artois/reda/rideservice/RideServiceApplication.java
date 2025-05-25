package fr.univ.artois.reda.userservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.r2dbc.config.EnableR2dbcAuditing;
import reactor.blockhound.BlockHound;

@SpringBootApplication
@EnableR2dbcAuditing
public class RideServiceApplication {
    public static void main(String[] args) {
        BlockHound.install();
        SpringApplication.run(RideServiceApplication.class, args);
    }
}
