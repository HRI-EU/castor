/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.cache.CacheKeyPrefix;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class ReservationCachingService {
  public static final String NO_RESERVATION_FOR_ID_EXCEPTION_MSG =
      "No reservation was found for requestId %s.";
  public static final String RESERVATION_CONFLICT_EXCEPTION_MSG =
      "Reservation conflict. Reservation with ID #%s already exists.";
  public static final String RE_CANNOT_BE_SATISFIED_EXCEPTION_FORMAT =
      "No fragment found to fulfill given reservation: %s";
  public static final String FAILED_UPDATING_RESERVATION_EXCEPTION_MSG =
      "Failed updating reservation marker for chunk #%s.";
  private final ConsumptionCachingService consumptionCachingService;
  private final RedisTemplate<String, Object> redisTemplate;
  private final TupleChunkFragmentStorageService tupleChunkFragmentStorageService;
  private final String cachePrefix;

  @Autowired
  public ReservationCachingService(
      CastorCacheProperties castorCacheProperties,
      ConsumptionCachingService consumptionCachingService,
      RedisTemplate<String, Object> redisTemplate,
      TupleChunkFragmentStorageService tupleChunkFragmentStorageService) {
    this.consumptionCachingService = consumptionCachingService;
    this.redisTemplate = redisTemplate;
    this.tupleChunkFragmentStorageService = tupleChunkFragmentStorageService;
    this.cachePrefix = CacheKeyPrefix.simple().compute(castorCacheProperties.getReservationStore());
  }

  /**
   * Stores the given {@link Reservation} in cache if no {@link Reservation} with the same ID is
   * present.
   *
   * <p>Reserving tuples will invoke tuple consumption (see {@link ConsumptionCachingService}) since
   * the related tuples are no longer available for other purpose.
   *
   * @throws CastorServiceException if the tuples could not be reserved as requested.
   * @throws CastorServiceException if the cache already holds a reservation with the given ID
   */
  @Transactional
  public void keepReservation(Reservation reservation) {
    log.debug("persisting reservation {}", reservation);
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    if (ops.get(cachePrefix + reservation.getReservationId()) == null) {
      ops.set(cachePrefix + reservation.getReservationId(), reservation);
      log.debug("put in database at {}", cachePrefix + reservation.getReservationId());
      applyReservationRequiresTransaction(reservation);
      log.debug("fragments updated.");
      consumptionCachingService.keepConsumption(
          System.currentTimeMillis(),
          reservation.getTupleType(),
          reservation.getReservations().stream()
              .mapToLong(ReservationElement::getReservedTuples)
              .sum());
      log.debug("consumption emitted");
    } else {
      throw new CastorServiceException(
          String.format(RESERVATION_CONFLICT_EXCEPTION_MSG, reservation.getReservationId()));
    }
  }

  /**
   * Reserve tuples as described by the given {@link Reservation}.
   *
   * @param reservation The {@link Reservation} to process.
   * @throws CastorServiceException if the tuples could not be reserved as requested.
   */
  protected void applyReservationRequiresTransaction(Reservation reservation) {
    log.debug("Apply reservation {}", reservation);
    for (ReservationElement re : reservation.getReservations()) {
      log.debug("Processing reservation element {}", re);
      long startIndex = re.getStartIndex();
      long endIndex = startIndex + re.getReservedTuples();
      while (startIndex < endIndex) {
        TupleChunkFragmentEntity fragment =
            tupleChunkFragmentStorageService
                .findAvailableFragmentForChunkContainingIndex(re.getTupleChunkId(), startIndex)
                .orElseThrow(
                    () ->
                        new CastorServiceException(
                            String.format(RE_CANNOT_BE_SATISFIED_EXCEPTION_FORMAT, reservation)));
        if (fragment.getStartIndex() < startIndex) {
          fragment = tupleChunkFragmentStorageService.splitBefore(fragment, startIndex);
        }
        if (endIndex < fragment.getEndIndex()) {
          fragment = tupleChunkFragmentStorageService.splitAt(fragment, endIndex);
        }
        fragment.setReservationId(reservation.getReservationId());
        tupleChunkFragmentStorageService.update(fragment);
        startIndex = fragment.getEndIndex();
      }
    }
  }

  /**
   * Updates the status of a {@link Reservation} with the given id cache.
   *
   * @param reservationId Id of the {@link Reservation} to update.
   * @param status the new {@link ActivationStatus} to be applied on the stored reservation
   * @throws CastorServiceException if no {@link Reservation} is associated with the given
   *     reservation's ID
   */
  @Transactional
  public void updateReservation(String reservationId, ActivationStatus status) {
    log.debug("updating reservation {}", reservationId);
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    Object value = ops.get(cachePrefix + reservationId);
    log.debug("object in cache at {} is {}", cachePrefix + reservationId, value);
    Reservation reservation = (value != null) ? (Reservation) value : null;
    if (reservation != null) {
      reservation.setStatus(status);
      ops.set(cachePrefix + reservation.getReservationId(), reservation);
      log.debug("reservation updated");
    } else {
      throw new CastorServiceException(
          String.format(NO_RESERVATION_FOR_ID_EXCEPTION_MSG, reservationId));
    }
  }

  /**
   * @return the {@link Reservation} with the given ID from cache, or null if no {@link Reservation}
   *     is associated with the specified ID
   */
  @Nullable
  @Transactional(readOnly = true)
  public Reservation getReservation(String reservationId) {
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    return (Reservation) ops.get(cachePrefix + reservationId);
  }

  @Transactional
  public void forgetReservation(String reservationId) {
    redisTemplate.delete(cachePrefix + reservationId);
  }
}
