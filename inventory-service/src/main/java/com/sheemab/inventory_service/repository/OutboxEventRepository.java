package com.sheemab.inventory_service.repository;


import com.sheemab.inventory_service.entity.OutboxEvent;
import com.sheemab.inventory_service.enums.OutboxStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
        SELECT e FROM OutboxEvent e
        WHERE e.status = :status
          AND e.createdAt <= :before
        ORDER BY e.createdAt ASC
        """)
    List<OutboxEvent> findPendingWithLock(
            @Param("status") OutboxStatus status,
            @Param("before") LocalDateTime before
    );

    long countByStatus(OutboxStatus status);

    @Query("""
        SELECT e FROM OutboxEvent e
        WHERE e.status = 'PROCESSED'
          AND e.processedAt < :cutoff
        """)
    List<OutboxEvent> findProcessedBefore(@Param("cutoff") LocalDateTime cutoff);
}
