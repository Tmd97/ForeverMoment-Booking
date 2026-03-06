package com.forvmom.MomentForeverBooking.scheduler;
import com.forvmom.MomentForeverBooking.service.retries_cleanup.OutgoingOutboxPublisher;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.springframework.stereotype.Component;

@Component
public class OutgoingOutboxPublisherJob implements Job {

    private final OutgoingOutboxPublisher publisher;

    public OutgoingOutboxPublisherJob(OutgoingOutboxPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void execute(JobExecutionContext context) {
        publisher.publishPendingEvents();
    }
}