package fr.univ.artois.reda.driverservice.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.ConnectableFlux;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class KafkaFluxRegistry {

    private final Map<String, ConnectableFlux<? extends ReceiverRecord<String, ? extends SpecificRecord>>> connectableFluxMap;

    public KafkaFluxRegistry() {
        this.connectableFluxMap = new ConcurrentHashMap<>();
    }

    public <T extends SpecificRecord> void registerFlux(String topic, ReactiveKafkaConsumerTemplate<String, T> template) {
        if (connectableFluxMap.containsKey(topic)) {
            return;
        }

        ConnectableFlux<ReceiverRecord<String, T>> connectableFlux = template
                .receive()
                .doOnError(error -> log.error("Error receiving event on topic {}: {}", topic, error.getMessage()))
                .log()
                .publish();

        connectableFluxMap.put(topic, connectableFlux);
    }

    @SuppressWarnings("unchecked")
    public <T extends SpecificRecord> ConnectableFlux<ReceiverRecord<String, T>> getFlux(String topic) {
        return (ConnectableFlux<ReceiverRecord<String, T>>) connectableFluxMap.get(topic);
    }
}
