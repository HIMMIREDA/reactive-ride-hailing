package fr.univ.artois.reda.authservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.r2dbc.config.EnableR2dbcAuditing;
import reactor.blockhound.BlockHound;

@SpringBootApplication
@EnableR2dbcAuditing
public class AuthServiceApplication {
    public static void main(String[] args) {
        BlockHound.install();
        SpringApplication.run(AuthServiceApplication.class, args);
    }
}
