package com.forvmom.MomentForeverBooking.scheduler;

import com.forvmom.MomentForeverBooking.service.OutboxRetryService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OutboxRetriesJob implements Job {

    @Autowired
    private OutboxRetryService outboxRetryService;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        outboxRetryService.retryStuckAndFailedRecords();
    }
}
