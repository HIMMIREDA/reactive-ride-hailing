package fr.univ.artois.reda.rideservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.r2dbc.config.EnableR2dbcAuditing;
import reactor.blockhound.BlockHound;

@SpringBootApplication
@EnableR2dbcAuditing
public class RideServiceApplication {
    public static void main(String[] args) {
        BlockHound.builder()
                .allowBlockingCallsInside("sun.security.ssl.SSLContextImpl", "engineInit")
                .install();
        SpringApplication.run(RideServiceApplication.class, args);
    }
}
