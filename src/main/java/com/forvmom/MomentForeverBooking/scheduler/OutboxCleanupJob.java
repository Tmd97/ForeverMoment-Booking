package com.forvmom.MomentForeverBooking.scheduler;
import com.forvmom.MomentForeverBooking.service.retries_cleanup.InboundOutboxCleanupService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OutboxCleanupJob implements Job {

    @Autowired
    private InboundOutboxCleanupService inboundOutboxCleanupService;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        inboundOutboxCleanupService.cleanupPublishedRecords();
    }
}
