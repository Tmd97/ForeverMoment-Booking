package com.forvmom.MomentForeverBooking.scheduler;

import com.forvmom.MomentForeverBooking.service.OutgoingOutboxService;
import com.forvmom.MomentForeverBooking.service.retries_cleanup.InboundOutboxRetryService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OutboxRetriesJob implements Job {

    @Autowired
    private InboundOutboxRetryService inboundOutboxRetryService;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        inboundOutboxRetryService.retryStuckAndFailedRecords();
    }
}
