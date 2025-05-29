package fr.univ.artois.reda.driverservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import reactor.blockhound.BlockHound;

@SpringBootApplication
@EnableReactiveMongoRepositories(createIndexesForQueryMethods = true)
@EnableMongoAuditing
public class DriverServiceApplication {
    public static void main(String[] args) {
        BlockHound.builder()
                .allowBlockingCallsInside("sun.security.ssl.SSLContextImpl", "engineInit")
                .install();
        SpringApplication.run(DriverServiceApplication.class, args);
    }
}
