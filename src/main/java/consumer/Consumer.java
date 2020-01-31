package consumer;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import quartz.QuartzData;

public class Consumer {
    public static void main(String[] args) throws SchedulerException {
        QuartzData quartzData = new QuartzData();
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        Scheduler scheduler = schedulerFactory.getScheduler();

        scheduler.scheduleJob(quartzData.getJobDetail(), quartzData.getTrigger());
        scheduler.start();
    }
}
