package io.github.himmireda.ridehailing.driverservice.changestream;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

@Configuration
@EnableConfigurationProperties(ChangeStreamProperties.class)
public class ChangeStreamConfig {

    @Bean
    public RideRequestChangeStream rideRequestChangeStream(ReactiveMongoTemplate mongoTemplate,
                                                           ChangeStreamProperties properties) {
        return new RideRequestChangeStream(mongoTemplate, properties);
    }
}
