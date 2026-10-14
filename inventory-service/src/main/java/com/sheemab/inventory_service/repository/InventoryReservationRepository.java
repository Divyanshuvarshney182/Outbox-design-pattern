package com.sheemab.inventory_service.repository;


import com.sheemab.inventory_service.entity.InventoryReservation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface InventoryReservationRepository extends JpaRepository<InventoryReservation, String> {

    /**
     * Used for idempotency — if a reservation for this orderId already exists,
     * return it rather than creating a duplicate.
     */
    Optional<InventoryReservation> findByOrderId(String orderId);

    List<InventoryReservation> findByProductId(String productId);
}
