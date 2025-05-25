package fr.univ.artois.reda.rideservice.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Value("${kafka.topics.ride-requested}")
    private String rideRequestedTopicName;

    @Value("${kafka.topics.ride-to-accept}")
    private String rideToAcceptTopicName;

    @Value("${kafka.topics.ride-booked}")
    private String rideBookedTopicName;

    @Bean
    public NewTopic rideRequestedTopic(){
        return new NewTopic(rideRequestedTopicName, 1, (short) 1);
    }
    @Bean
    public NewTopic rideAcceptedTopic(){
        return new NewTopic(rideToAcceptTopicName, 1, (short) 1);
    }
    @Bean
    public NewTopic rideBookedTopic(){
        return new NewTopic(rideBookedTopicName, 1, (short) 1);
    }

}
