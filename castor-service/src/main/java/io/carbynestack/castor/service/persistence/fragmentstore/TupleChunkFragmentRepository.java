/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.fragmentstore;

import static io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity.*;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.TupleType;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface TupleChunkFragmentRepository
    extends CrudRepository<TupleChunkFragmentEntity, Long> {
  @Transactional(readOnly = true)
  Optional<TupleChunkFragmentEntity>
      findFirstByTupleTypeAndActivationStatusAndReservationIdNullOrderByIdAsc(
          TupleType tupleType, ActivationStatus activationStatus);

  @Transactional(readOnly = true)
  @Query(
      value =
          "SELECT * FROM "
              + TABLE_NAME
              + " WHERE "
              + TUPLE_CHUNK_ID_COLUMN
              + "=:tupleChunkId "
              + "AND "
              + ACTIVATION_STATUS_COLUMN
              + "='UNLOCKED' "
              + "AND "
              + RESERVATION_ID_COLUMN
              + " IS NULL "
              + "AND "
              + START_INDEX_COLUMN
              + "<=:startIndex "
              + "AND "
              + END_INDEX_COLUMN
              + ">:startIndex"
              + " ORDER BY "
              + START_INDEX_COLUMN
              + " DESC"
              + " LIMIT 1",
      nativeQuery = true)
  Optional<TupleChunkFragmentEntity> findAvailableFragmentForTupleChunkContainingIndex(
      @Param("tupleChunkId") UUID tupleChunkId, @Param("startIndex") long startIndex);

  @Transactional(readOnly = true)
  @Query(
      value =
          "SELECT * FROM "
              + TABLE_NAME
              + " WHERE "
              + TUPLE_CHUNK_ID_COLUMN
              + "=:tupleChunkId "
              + "AND "
              + START_INDEX_COLUMN
              + "<:endIndex "
              + "AND "
              + END_INDEX_COLUMN
              + " >:startIndex "
              + "LIMIT 1",
      nativeQuery = true)
  Optional<TupleChunkFragmentEntity> findFirstFragmentContainingAnyTupleOfSequence(
      @Param("tupleChunkId") UUID tupleChunkId,
      @Param("startIndex") long startIndex,
      @Param("endIndex") long endIndex);

  @Transactional(readOnly = true)
  @Query(
      value =
          "SELECT SUM ("
              + END_INDEX_FIELD
              + " - "
              + START_INDEX_FIELD
              + ")"
              + " FROM "
              + CLASS_NAME
              + " WHERE "
              + ACTIVATION_STATUS_FIELD
              + "=io.carbynestack.castor.common.entities.ActivationStatus.UNLOCKED "
              + "AND "
              + RESERVATION_ID_FIELD
              + " IS NULL "
              + "AND "
              + TUPLE_TYPE_FIELD
              + "=:tupleType")
  long getAvailableTupleByType(@Param("tupleType") TupleType type);

  @Transactional
  @Modifying
  @Query(
      value =
          "UPDATE "
              + TABLE_NAME
              + " SET "
              + ACTIVATION_STATUS_COLUMN
              + "='UNLOCKED' WHERE "
              + TUPLE_CHUNK_ID_COLUMN
              + " = :tupleChunkId",
      nativeQuery = true)
  int unlockAllForTupleChunk(@Param("tupleChunkId") UUID tupleChunkId);

  @Transactional
  void deleteAllByReservationId(String reservationId);

  @Transactional
  boolean existsByTupleChunkId(UUID tupleChunkId);
}
