package com.forvmom.MomentForeverBooking.config;
import com.forvmom.MomentForeverBooking.scheduler.OutboxCleanupJob;
import com.forvmom.MomentForeverBooking.scheduler.OutboxRetriesJob;
import com.forvmom.MomentForeverBooking.scheduler.OutgoingOutboxPublisherJob;
import org.quartz.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
// we are using fire now policy, so even if many missed fires, we fire only once when server
// heal back, as once fire only is actually trigger all missed enrich process. so good for me
@Configuration
public class QuartzConfig {

    private static final int CLEANUP_INTERVAL_HOURS = 24;
    private static final int RETRY_INTERVAL_MINUTES = 1;

    @Bean
    public JobDetail outboxCleanupJobDetail() {
        return JobBuilder.newJob(OutboxCleanupJob.class)
                .withIdentity("outboxCleanupJob")
                .storeDurably()
                .build();
    }

    @Bean
    public Trigger outboxCleanupTrigger() {
        SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder.simpleSchedule()
                .withIntervalInHours(CLEANUP_INTERVAL_HOURS)
                .repeatForever()
                .withMisfireHandlingInstructionFireNow(); // Important: on misfire, run immediately

        return TriggerBuilder.newTrigger()
                .forJob(outboxCleanupJobDetail())
                .withIdentity("outboxCleanupTrigger")
                .withDescription("Runs every 24 hours to clean up old published records")
                .withSchedule(scheduleBuilder)
                .build();
    }

    @Bean
    public JobDetail outboxRetryJobDetail() {
        return JobBuilder.newJob(OutboxRetriesJob.class)
                .withIdentity("outboxRetryJob")
                .storeDurably()
                .build();
    }

    @Bean
    public Trigger outboxRetryTrigger() {
        SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder.simpleSchedule()
                .withIntervalInMinutes(RETRY_INTERVAL_MINUTES)
                .repeatForever()
                .withMisfireHandlingInstructionFireNow();

        return TriggerBuilder.newTrigger()
                .forJob(outboxRetryJobDetail())
                .withIdentity("outboxRetryTrigger")
                .withDescription("Retries failed/unprocessed outbox records older than 5 minutes")
                .withSchedule(scheduleBuilder)
                .build();
    }

    /// //////////Outgoing events Job & Triggers////////////////////////////

    @Bean
    public JobDetail outgoingOutboxPublisherJobDetail() {
        return JobBuilder.newJob(OutgoingOutboxPublisherJob.class)
                .withIdentity("outgoingOutboxPublisherJob")
                .storeDurably()
                .build();
    }

    @Bean
    public Trigger outgoingOutboxPublisherTrigger() {
        SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder.simpleSchedule()
                .withIntervalInSeconds(30)   // runs every 30 seconds
                .repeatForever()
                .withMisfireHandlingInstructionFireNow();

        return TriggerBuilder.newTrigger()
                .forJob(outgoingOutboxPublisherJobDetail())
                .withIdentity("outgoingOutboxPublisherTrigger")
                .withSchedule(scheduleBuilder)
                .build();
    }
}