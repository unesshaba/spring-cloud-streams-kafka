package org.sid.springcloudstreamskafka.service;

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
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;


@Service
public class PageEventService {
    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (input)->{
            System.out.println("**************************");
            System.out.println(input.toString());
            System.out.println("****************************");
        };
    }
    @Bean   //chaque 1s va executer cette function
    public Supplier<PageEvent> pageEventSupplier(){
        return()->new PageEvent(
                Math.random()>0.5?"P1":"P2",
                Math.random()>0.5?"u1":"u2",
                new Date(),
                new Random().nextInt(9000));
    }
    @Bean
    //peut 2eme parametre ikon chi autre Object(out) li aykon fih resultat de taitemet de PageEvent(in)
    public Function<PageEvent,PageEvent> PageEventFunction(){
        return(input)->{
            //modifier l'objet (In) TO Object (Out)
            input.setName("New Page Event");
            input.setUser("new user");
            return input;

        };
    }
    /*//@Bean
    public Function<KStream<String,PageEvent>,KStream<String,Long>> StreamFunction(){
        return (input)->{
            return input
                    .filter((k,v)->v.getDuration()>100)
                    .map((k,v)->new KeyValue<>(v.getName(),0L))
                    .groupBy((k,v)->k,Grouped.with(Serdes.String(),Serdes.Long())) //va retourner un KTable
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(50))) //va retourner un window a la place de String pour l'attribut Key
                    // cette methode-> a chaque 5s va renitialiser le nombre de visite
                    .count(Materialized.as("page-count"))
                    .toStream()  //pour converty Ktable de GroupBy to KStream
                    .map((k,v)->new
                            KeyValue<>("=>"+k.window().startTime()+" - " +k.window().endTime()+"  "+k.key(), v));
        };
    }*/
}
