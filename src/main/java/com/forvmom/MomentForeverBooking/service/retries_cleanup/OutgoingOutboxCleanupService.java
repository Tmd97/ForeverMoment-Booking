package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.repository.OutgoingOutboxDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
public class OutgoingOutboxCleanupService {

    private static final Logger log = LoggerFactory.getLogger(OutgoingOutboxCleanupService.class);
    private static final int HOURS_TO_KEEP = 24;

    private final OutgoingOutboxDao outgoingOutboxDao;

    public OutgoingOutboxCleanupService(OutgoingOutboxDao outgoingOutboxDao) {
        this.outgoingOutboxDao = outgoingOutboxDao;
    }

    @Transactional
    public void cleanupSentRecords() {
        LocalDateTime cutoff = LocalDateTime.now().minusHours(HOURS_TO_KEEP);
        int deleted = outgoingOutboxDao.deleteByStatusAndUpdatedAtBefore(OutgoingOutboxRecord.STATUS_SENT, cutoff);
        if (deleted > 0) {
            log.info("Cleaned up {} sent outgoing outbox records older than {}", deleted, cutoff);
        }
    }
}