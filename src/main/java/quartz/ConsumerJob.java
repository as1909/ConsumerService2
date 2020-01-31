package quartz;

import com.fasterxml.jackson.databind.ObjectMapper;
import consts.Const;
import model.Trade;
import model.TradeDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static consts.Const.FILES_DIRECTORY;
import static consts.Const.KAFKA_BOOTSTRAP_SERVERS;
import static consts.Const.KAFKA_GROUP;

public class ConsumerJob implements Job {

    public void execute(JobExecutionContext jobExecutionContext) {
        JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        final Consumer<String, Trade> consumer = createConsumer(dataMap);
        final ConsumerRecords<String, Trade> consumerRecords = consumer.poll(Duration.ofSeconds(5));

        List<Trade> trades = new ArrayList<>();

        Iterator<ConsumerRecord<String, Trade>> records = consumerRecords.iterator();
        while(records.hasNext()) {
            trades.add(records.next().value());
        }
        trades.forEach(System.out::println);

        writeTofile(trades, dataMap.getString(FILES_DIRECTORY));
        consumer.close();
    }

    private void writeTofile(List<Trade> trades, String directory) {
        ObjectMapper objectMapper = new ObjectMapper();
        trades.stream().forEach(trade -> {
            try {
                String fileName = directory + System.currentTimeMillis() + ".json";
                objectMapper.writeValue(new File(fileName), trade);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }

    private static Consumer<String, Trade> createConsumer(JobDataMap dataMap) {
        Properties properties = getProperties(dataMap);

        final Consumer<String, Trade> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(dataMap.getString(Const.KAFKA_TOPIC)));
        return consumer;
    }

    private static Properties getProperties(JobDataMap dataMap) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, dataMap.getString(KAFKA_BOOTSTRAP_SERVERS));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TradeDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP);
        return properties;
    }


}
