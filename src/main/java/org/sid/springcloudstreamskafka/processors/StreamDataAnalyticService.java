package org.sid.springcloudstreamskafka.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.sid.springcloudstreamskafka.entities.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.function.Function;

@Service
public class StreamDataAnalyticService {
    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Double>> kStreamFunction2(){
        return (input)->input
                .map((k,v)->new KeyValue<>(v.getName(), (double)v.getDuration()))
                .groupBy((k,v)->k, Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(30)))
                .aggregate(()->0.0, (k,v,total)->total+v, Materialized.as("total-store"))
                .toStream()
                .map((k,v)->new KeyValue<>(k.key().toString(),v));
    }
}
