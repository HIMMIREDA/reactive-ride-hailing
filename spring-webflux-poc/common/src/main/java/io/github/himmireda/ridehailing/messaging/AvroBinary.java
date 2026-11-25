package io.github.himmireda.ridehailing.messaging;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Plain Avro binary encoding of the generated event classes, used to store an event in an outbox
 * until it is published. No Schema Registry framing: the relay re-serializes the decoded object with
 * KafkaAvroSerializer, so what reaches Kafka is exactly what a direct send would have produced.
 */
public final class AvroBinary {

    private AvroBinary() {
    }

    public static byte[] encode(SpecificRecord event) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new SpecificDatumWriter<SpecificRecord>(event.getSchema()).write(event, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not encode " + event.getSchema().getFullName(), e);
        }
    }

    /** @param eventType the full schema name, as returned by {@link #eventType(SpecificRecord)} */
    public static SpecificRecord decode(String eventType, byte[] payload) {
        try {
            Class<?> type = Class.forName(eventType);
            Schema schema = SpecificData.get().getSchema(type);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
            return new SpecificDatumReader<SpecificRecord>(schema).read(null, decoder);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unknown event type " + eventType, e);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not decode " + eventType, e);
        }
    }

    public static String eventType(SpecificRecord event) {
        return event.getSchema().getFullName();
    }
}
