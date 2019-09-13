package com.azb.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.dsl.kafka.Kafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.PollableChannel;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
 
@SpringBootApplication
public class SubscribedReader {
 
    @Autowired
    PollableChannel consumerChannel;
 
    public static void main(String[] args) {
 
        ConfigurableApplicationContext context = new SpringApplicationBuilder(SubscribedReader.class).run(args);
 
        List valid_topics = Arrays.asList("fantasy", "horror", "romance", "thriller");
 
        List topics = Arrays.stream(args)
                .filter(valid_topics::contains)
                .collect(Collectors.toList());
 
        context.getBean(SubscribedReader.class).run(context, topics);
        context.close();
    }
 
    private void run(ConfigurableApplicationContext context, List<String> topics) {
 
        System.out.println("Inside ConsumerApplication run method...");
        PollableChannel consumerChannel = context.getBean("consumerChannel", PollableChannel.class);

        topics.forEach(this::addAnotherListenerForTopics);
 
        Message received = consumerChannel.receive();
        while (received != null) {
            received = consumerChannel.receive();
            System.out.println("Received " + received.getPayload());
        }
    }
 
    @Autowired
    private IntegrationFlowContext flowContext;
 
    @Autowired
    private KafkaProperties kafkaProperties;
 
    public void addAnotherListenerForTopics(String... topics) {
        Map consumerProperties = kafkaProperties.buildConsumerProperties();
        IntegrationFlow flow = IntegrationFlows
                .from(Kafka.messageDrivenChannelAdapter(
                        new DefaultKafkaConsumerFactory(consumerProperties), topics))
                .channel("consumerChannel").get();
        this.flowContext.registration(flow).register();
    }
}