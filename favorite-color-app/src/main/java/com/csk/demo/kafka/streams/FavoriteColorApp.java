package com.csk.demo.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        final StreamsBuilder builder = new StreamsBuilder();



        builder.table("favorite-color-input-topic")
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Named.as("user-colors-count"))
                .toStream()
                .to("favorite-color-output-topic");

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

        //Graceful Shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));


    }
}
