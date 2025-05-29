package fr.univ.artois.reda.driverservice.kafka.consumer;

import fr.univ.artois.reda.driverservice.kafka.KafkaFluxRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RideToAcceptEventConsumer {
    private final KafkaFluxRegistry kafkaFluxRegistry;

    @Value("${kafka.topics.ride-to-accept}")
    private String rideToAcceptTopicName;


    @EventListener(ApplicationReadyEvent.class)
    public void startKafkaConsumer() {
        kafkaFluxRegistry.getFlux(rideToAcceptTopicName)
                .autoConnect()
                .doOnNext(r ->{
                    System.out.println("Received event on topic " + rideToAcceptTopicName + ": " + r.value());
                    r.receiverOffset().acknowledge();
                })
                .subscribe();
    }
}
