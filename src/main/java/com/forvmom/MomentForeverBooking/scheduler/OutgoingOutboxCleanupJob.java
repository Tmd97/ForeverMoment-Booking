package com.forvmom.MomentForeverBooking.scheduler;

import com.forvmom.MomentForeverBooking.service.retries_cleanup.OutgoingOutboxCleanupService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.springframework.stereotype.Component;

@Component
public class OutgoingOutboxCleanupJob implements Job {

    private final OutgoingOutboxCleanupService outgoingOutboxCleanupService;

    public OutgoingOutboxCleanupJob(OutgoingOutboxCleanupService outgoingOutboxCleanupService) {
        this.outgoingOutboxCleanupService = outgoingOutboxCleanupService;
    }

    @Override
    public void execute(JobExecutionContext context) {
        outgoingOutboxCleanupService.cleanupSentRecords();
    }
}