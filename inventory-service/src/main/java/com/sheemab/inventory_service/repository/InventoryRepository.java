package com.sheemab.inventory_service.repository;


import com.sheemab.inventory_service.entity.Inventory;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface InventoryRepository extends JpaRepository<Inventory, String> {

    Optional<Inventory> findByProductId(String productId);

    /**
     * Fetch by productId with a pessimistic write lock.
     * This prevents two concurrent reservation requests for the same product
     * from both seeing availableQuantity = 10 and both succeeding — only
     * one transaction can hold the lock at a time.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT i FROM Inventory i WHERE i.productId = :productId")
    Optional<Inventory> findByProductIdWithLock(@Param("productId") String productId);
}