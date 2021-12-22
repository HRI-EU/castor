/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import static io.carbynestack.castor.service.persistence.cache.ReservationCachingService.*;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.cache.CacheKeyPrefix;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@RunWith(MockitoJUnitRunner.class)
public class ReservationCachingServiceTest {
  @Mock private ConsumptionCachingService consumptionCachingServiceMock;

  @Mock private RedisTemplate<String, Object> redisTemplateMock;

  @Mock private ValueOperations<String, Object> valueOperationsMock;

  @Mock private CastorCacheProperties castorCachePropertiesMock;

  @Mock private TupleChunkFragmentStorageService tupleChunkFragmentStorageServiceMock;

  private final String testCacheName = "testCache";
  private final String testCachePrefix = CacheKeyPrefix.simple().compute(testCacheName);

  private ReservationCachingService reservationCachingService;

  @Before
  public void setUp() {
    when(castorCachePropertiesMock.getReservationStore()).thenReturn(testCacheName);
    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
    this.reservationCachingService =
        new ReservationCachingService(
            castorCachePropertiesMock,
            consumptionCachingServiceMock,
            redisTemplateMock,
            tupleChunkFragmentStorageServiceMock);
  }

  @Test
  public void givenReservationCannotBeFulfilled_whenKeepReservation_throwCastorServiceException() {
    UUID requestedChunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    TupleType requestedTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long requestedStartIndex = 0;
    long requestedNumberOfTuples = 42;
    ReservationElement reservationElement =
        new ReservationElement(requestedChunkId, requestedNumberOfTuples, requestedStartIndex);
    Reservation reservationMock = mock(Reservation.class);
    CastorServiceException expectedException = new CastorServiceException("expected");

    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElement));
    when(tupleChunkFragmentStorageServiceMock.findAvailableFragmentForChunkContainingIndex(
            requestedChunkId, requestedStartIndex))
        .thenThrow(expectedException);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.keepReservation(reservationMock));

    assertEquals(expectedException, actualCse);
  }

  @Test
  public void
      givenNoReservationWithGivenIdInCache_whenKeepReservation_thenStoreInCacheAndEmitConsumption() {
    long consumption = 42;
    long startIndex = 0;
    UUID chunkId = UUID.fromString("b7b010e0-362b-401c-9560-4cf4b2a68139");
    ReservationElement reservationElementMock = mock(ReservationElement.class);
    String reservationId = "reservationId";
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    Reservation reservationMock = mock(Reservation.class);
    TupleChunkFragmentEntity fragmentEntityMock = mock(TupleChunkFragmentEntity.class);

    when(reservationElementMock.getTupleChunkId()).thenReturn(chunkId);
    when(reservationElementMock.getReservedTuples()).thenReturn(consumption);
    when(reservationElementMock.getStartIndex()).thenReturn(startIndex);
    when(reservationMock.getReservationId()).thenReturn(reservationId);
    when(reservationMock.getTupleType()).thenReturn(tupleType);
    when(reservationMock.getReservations()).thenReturn(singletonList(reservationElementMock));
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(null);
    when(fragmentEntityMock.getStartIndex()).thenReturn(startIndex);
    when(fragmentEntityMock.getEndIndex()).thenReturn(consumption);
    when(tupleChunkFragmentStorageServiceMock.findAvailableFragmentForChunkContainingIndex(
            chunkId, startIndex))
        .thenReturn(Optional.of(fragmentEntityMock));

    reservationCachingService.keepReservation(reservationMock);

    verify(valueOperationsMock).set(testCachePrefix + reservationId, reservationMock);
    verify(consumptionCachingServiceMock)
        .keepConsumption(anyLong(), eq(tupleType), eq(consumption));
  }

  @Test
  public void
      givenReservationWithSameIdInCache_whenKeepReservation_thenThrowCastorServiceException() {
    String reservationId = "reservationId";
    Reservation reservationMock = mock(Reservation.class);

    when(reservationMock.getReservationId()).thenReturn(reservationId);
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(reservationMock);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.keepReservation(reservationMock));

    assertEquals(
        String.format(RESERVATION_CONFLICT_EXCEPTION_MSG, reservationId), actualCse.getMessage());
    verify(valueOperationsMock, never()).set(anyString(), any(Reservation.class));
    verify(consumptionCachingServiceMock, never())
        .keepConsumption(anyLong(), any(TupleType.class), anyLong());
  }

  @Test
  public void
      givenNoReservationWithIdInCache_whenUpdateReservation_thenThrowCastorServiceException() {
    String reservationId = "reservationId";
    ActivationStatus newStatus = ActivationStatus.UNLOCKED;

    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(null);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> reservationCachingService.updateReservation(reservationId, newStatus));

    assertEquals(
        String.format(NO_RESERVATION_FOR_ID_EXCEPTION_MSG, reservationId), actualCse.getMessage());
  }

  @Test
  public void givenSuccessfulRequest_whenUpdateReservation_thenUpdateEntityInCache() {
    String reservationId = "reservationId";
    Reservation reservationMock = mock(Reservation.class);
    ActivationStatus newStatus = ActivationStatus.UNLOCKED;

    when(reservationMock.getReservationId()).thenReturn(reservationId);
    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(reservationMock);

    reservationCachingService.updateReservation(reservationId, newStatus);

    verify(valueOperationsMock).set(testCachePrefix + reservationId, reservationMock);
    verify(reservationMock).setStatus(newStatus);
  }

  @Test
  public void givenReservationWithIdInCache_whenGetReservation_thenReturnExpectedReservation() {
    String reservationId = "reservationId";
    Reservation reservationMock = mock(Reservation.class);

    when(valueOperationsMock.get(testCachePrefix + reservationId)).thenReturn(reservationMock);

    assertEquals(reservationMock, reservationCachingService.getReservation(reservationId));
  }

  @Test
  public void givenSuccessfulRequest_whenForgetReservation_thenCallDeleteOnCache() {
    String reservationId = "reservationId";

    reservationCachingService.forgetReservation(reservationId);

    verify(redisTemplateMock).delete(testCachePrefix + reservationId);
  }

  @Test
  public void whenReferencedSequenceLiesWithin_whenApplyReservation_thenSplitFragmentAccordingly() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long requestedStartIndex = 42;
    long requestedLength = 21;
    ReservationElement re =
        new ReservationElement(tupleChunkId, requestedLength, requestedStartIndex);
    String reservationId = "testReservation";
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    Reservation r = new Reservation(reservationId, tupleType, Collections.singletonList(re));
    long existingFragmentStartIndex = 0;
    long existingFragmentEndIndex = 99;
    TupleChunkFragmentEntity existingFragment =
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            existingFragmentStartIndex,
            existingFragmentEndIndex,
            ActivationStatus.UNLOCKED,
            null);

    when(tupleChunkFragmentStorageServiceMock.splitAt(
            existingFragment, requestedStartIndex + requestedLength))
        .thenReturn(existingFragment);
    when(tupleChunkFragmentStorageServiceMock.splitBefore(existingFragment, requestedStartIndex))
        .thenReturn(existingFragment);
    when(tupleChunkFragmentStorageServiceMock.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, requestedStartIndex))
        .thenReturn(Optional.of(existingFragment));

    reservationCachingService.applyReservationRequiresTransaction(r);
    verify(tupleChunkFragmentStorageServiceMock, times(1)).update(existingFragment);
    verify(tupleChunkFragmentStorageServiceMock, times(1))
        .splitBefore(existingFragment, requestedStartIndex);
    verify(tupleChunkFragmentStorageServiceMock, times(1))
        .splitAt(existingFragment, requestedStartIndex + requestedLength);
  }
}
