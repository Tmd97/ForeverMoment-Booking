package com.forvmom.MomentForeverBooking.scheduler;
import com.forvmom.MomentForeverBooking.service.impl.OutboxCleanupService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OutboxRetriesJob implements Job {

    @Autowired
    private OutboxCleanupService outboxCleanupService;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        outboxCleanupService.retryStuckAndFailedRecords();
    }
}
