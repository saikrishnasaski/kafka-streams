package com.csk.demo.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

public class WordCountApp {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-word-count-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> wordsInputStream = builder.stream("words-input-topic");

        KTable<String, Long> wordsCountOutputStream = wordsInputStream
                .mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((key, value) -> value)
                .peek((key, value) -> {
                    System.out.println(key + " " + value);
                })
                .groupByKey()
                .count(Named.as("Counts"), Materialized.with(Serdes.String(), Serdes.Long()));

        wordsCountOutputStream.toStream()
                .to("words-count-output-topic", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        kafkaStreams.start();
        System.out.println(topology.describe());

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));


    }
}
