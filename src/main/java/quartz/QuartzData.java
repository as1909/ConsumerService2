package quartz;

import consumer.Consumer;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static consts.Const.FILES_DIRECTORY;
import static consts.Const.KAFKA_BOOTSTRAP_SERVERS;
import static consts.Const.KAFKA_GROUP;
import static consts.Const.KAFKA_TOPIC;
import static consts.Const.PROPERTIES;
import static consts.Const.QUARTZ_GROUP;
import static consts.Const.QUARTZ_INTERVAL_SEC;
import static consts.Const.QUARTZ_JOB;
import static consts.Const.QUARTZ_TRIGGER;

public class QuartzData {

    Properties properties = null;
    {
        try {
            properties = getProperties();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public JobDetail getJobDetail() {
        return JobBuilder.newJob(ConsumerJob.class)
                .withIdentity(QUARTZ_JOB, QUARTZ_GROUP)
                .usingJobData(FILES_DIRECTORY, properties.getProperty(FILES_DIRECTORY))
                .usingJobData(KAFKA_TOPIC, properties.getProperty(KAFKA_TOPIC))
                .usingJobData(KAFKA_BOOTSTRAP_SERVERS, properties.getProperty(KAFKA_BOOTSTRAP_SERVERS))
                .usingJobData(KAFKA_GROUP, properties.getProperty(KAFKA_GROUP))
                .build();
    }

    public Trigger getTrigger() {
        return TriggerBuilder.newTrigger()
                .withIdentity(QUARTZ_TRIGGER, QUARTZ_GROUP)
                .startNow()
                .withSchedule(SimpleScheduleBuilder
                        .simpleSchedule()
                        .withIntervalInSeconds(QUARTZ_INTERVAL_SEC)
                        .repeatForever())
                .build();
    }

    public Properties getProperties() throws IOException {
        try (InputStream input = Consumer.class.getClassLoader().getResourceAsStream(PROPERTIES)) {
            Properties prop = new Properties();
            prop.load(input);
            return prop;
        } catch (IOException ex) {
            throw new IOException("no properties found");
        }
    }
}
