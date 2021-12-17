/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.fragmentstore;

import static io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService.CONFLICT_EXCEPTION_MSG;
import static io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService.RE_CANNOT_BE_SATISFIED_EXCEPTION_FORMAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import java.util.*;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TupleChunkFragmentStorageServiceTest {
  @Mock private TupleChunkFragmentRepository tupleChunkFragmentRepositoryMock;

  @InjectMocks private TupleChunkFragmentStorageService tupleChunkFragmentStorageService;

  @Test
  public void
      givenNoFragmentInDbMatchingCriteria_whenFindAvailableFragment_thenReturnEmptyOptional() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long startIndex = 0;
    when(tupleChunkFragmentRepositoryMock.findAvailableFragmentForTupleChunkContainingIndex(
            tupleChunkId, startIndex))
        .thenReturn(Optional.empty());

    assertEquals(
        Optional.empty(),
        tupleChunkFragmentStorageService.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, startIndex));
  }

  @Test
  public void giveFragmentMatchesCriteria_whenFindAvailableFragment_thenReturnFragment() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long requestedStartIndex = 42;
    TupleChunkFragmentEntity actualFragmentMock = new TupleChunkFragmentEntity();

    when(tupleChunkFragmentRepositoryMock.findAvailableFragmentForTupleChunkContainingIndex(
            tupleChunkId, requestedStartIndex))
        .thenReturn(Optional.of(actualFragmentMock));

    assertEquals(
        Optional.of(actualFragmentMock),
        tupleChunkFragmentStorageService.findAvailableFragmentForChunkContainingIndex(
            tupleChunkId, requestedStartIndex));
  }

  @Test
  public void
      givenNoFragmentInDbMatchingCriteria_whenFindAvailableFragmentForType_thenReturnEmptyOptional() {
    TupleType requestedTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    when(tupleChunkFragmentRepositoryMock
            .findFirstByTupleTypeAndActivationStatusAndReservationIdNullOrderByIdAsc(
                requestedTupleType, ActivationStatus.UNLOCKED))
        .thenReturn(Optional.empty());

    assertEquals(
        Optional.empty(),
        tupleChunkFragmentStorageService.findAvailableFragmentWithTupleType(requestedTupleType));
  }

  @Test
  public void giveFragmentMatchesCriteria_whenFindAvailableFragmentForType_thenReturnFragment() {
    TupleType requestedTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    TupleChunkFragmentEntity actualFragmentMock = mock(TupleChunkFragmentEntity.class);

    when(tupleChunkFragmentRepositoryMock
            .findFirstByTupleTypeAndActivationStatusAndReservationIdNullOrderByIdAsc(
                requestedTupleType, ActivationStatus.UNLOCKED))
        .thenReturn(Optional.of(actualFragmentMock));

    assertEquals(
        Optional.of(actualFragmentMock),
        tupleChunkFragmentStorageService.findAvailableFragmentWithTupleType(requestedTupleType));
  }

  @Test
  public void givenConflictingFragment_whenCheckNoConflict_thenThrowException() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long startIndex = 42;
    long endIndex = 44;
    Optional<TupleChunkFragmentEntity> nonEmptyOptional =
        Optional.of(mock(TupleChunkFragmentEntity.class));

    when(tupleChunkFragmentRepositoryMock.findFirstFragmentContainingAnyTupleOfSequence(
            tupleChunkId, startIndex, endIndex))
        .thenReturn(nonEmptyOptional);

    CastorClientException actualCCE =
        Assert.assertThrows(
            CastorClientException.class,
            () ->
                tupleChunkFragmentStorageService.checkNoConflict(
                    tupleChunkId, startIndex, endIndex));
    assertEquals(CONFLICT_EXCEPTION_MSG, actualCCE.getMessage());
  }

  @Test
  public void givenNoConflictingFragment_whenCheckNoConflict_thenDoNothing() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long startIndex = 42;
    long endIndex = 44;

    when(tupleChunkFragmentRepositoryMock.findFirstFragmentContainingAnyTupleOfSequence(
            tupleChunkId, startIndex, endIndex))
        .thenReturn(Optional.empty());

    try {
      tupleChunkFragmentStorageService.checkNoConflict(tupleChunkId, startIndex, endIndex);
    } catch (Exception e) {
      Assert.fail("Method not expected to throw exception");
    }
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

    when(tupleChunkFragmentRepositoryMock.findAvailableFragmentForTupleChunkContainingIndex(
            tupleChunkId, requestedStartIndex))
        .thenReturn(Optional.of(existingFragment));

    tupleChunkFragmentStorageService.applyReservation(r);
    verify(tupleChunkFragmentRepositoryMock, times(1))
        .save(
            TupleChunkFragmentEntity.of(
                tupleChunkId,
                tupleType,
                existingFragmentStartIndex,
                requestedStartIndex,
                ActivationStatus.UNLOCKED,
                null));
    verify(tupleChunkFragmentRepositoryMock, times(1))
        .save(
            TupleChunkFragmentEntity.of(
                tupleChunkId,
                tupleType,
                requestedStartIndex,
                requestedStartIndex + requestedLength,
                ActivationStatus.UNLOCKED,
                reservationId));
    verify(tupleChunkFragmentRepositoryMock, times(1))
        .save(
            TupleChunkFragmentEntity.of(
                tupleChunkId,
                tupleType,
                requestedStartIndex + requestedLength,
                existingFragmentEndIndex,
                ActivationStatus.UNLOCKED,
                null));
  }

  @Test
  public void givenNoFragmentToFulfillElement_whenApplyReservation_thenThrowException() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long requestedStartIndex = 42;
    long requestedLength = 21;
    ReservationElement reservationElement =
        new ReservationElement(tupleChunkId, requestedLength, requestedStartIndex);
    String reservationId = "testReservation";
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    Reservation reservation =
        new Reservation(reservationId, tupleType, Collections.singletonList(reservationElement));

    when(tupleChunkFragmentRepositoryMock.findAvailableFragmentForTupleChunkContainingIndex(
            tupleChunkId, requestedStartIndex))
        .thenReturn(Optional.empty());
    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () -> tupleChunkFragmentStorageService.applyReservation(reservation));
    assertEquals(
        String.format(RE_CANNOT_BE_SATISFIED_EXCEPTION_FORMAT, reservation),
        actualCse.getMessage());
    verify(tupleChunkFragmentRepositoryMock, never()).save(any());
  }
}
