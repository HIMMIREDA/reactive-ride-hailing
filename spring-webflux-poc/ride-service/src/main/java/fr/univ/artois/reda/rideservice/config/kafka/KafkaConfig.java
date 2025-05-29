package fr.univ.artois.reda.rideservice.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Value("${kafka.topics.ride-request-requested}")
    private String rideRequestRequestedTopicName;

    @Value("${kafka.topics.ride-proposal-accepted}")
    private String rideProposalAcceptedTopicName;

    @Value("${kafka.topics.ride-proposal-booked}")
    private String rideProposalBookedTopicName;

    @Value("${kafka.topics.ride-proposal-proposed}")
    private String rideProposalProposedTopicName;

    @Bean
    public NewTopic rideRequestRequestedTopic() {
        return new NewTopic(rideRequestRequestedTopicName, 1, (short) 1);
    }

    @Bean
    public NewTopic rideProposalAcceptedTopic() {
        return new NewTopic(rideProposalAcceptedTopicName, 1, (short) 1);
    }

    @Bean
    public NewTopic rideProposalBookedTopic() {
        return new NewTopic(rideProposalBookedTopicName, 1, (short) 1);
    }

    @Bean
    public NewTopic rideProposalProposedTopic() {
        return new NewTopic(rideProposalProposedTopicName, 1, (short) 1);
    }

}

