/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.fragmentstore;

import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import java.util.Optional;
import java.util.UUID;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@AllArgsConstructor(onConstructor_ = @Autowired)
public class TupleChunkFragmentStorageService {
  public static final String CONFLICT_EXCEPTION_MSG =
      "At least one tuple described by the given sequence is already referenced by another"
          + " TupleChunkFragment";
  public static final String RE_CANNOT_BE_SATISFIED_EXCEPTION_FORMAT =
      "No fragment found to fulfill given reservation: %s";
  public static final String NOT_A_SINGLE_FRAGMENT_FOR_CHUNK_ERROR_MSG =
      "Not a single fragment associated with the given identifier.";

  private final TupleChunkFragmentRepository fragmentRepository;

  /**
   * Creates and stores an {@link TupleChunkFragmentEntity} object with the given information in the
   * database
   *
   * @param fragment The {@link TupleChunkFragmentEntity} to persist.
   * @return The stored / updated {@link TupleChunkFragmentEntity}.
   * @throws IllegalArgumentException In case the given fragment is null.
   * @throws CastorClientException If any of the tuples referenced by the given {@link
   *     TupleChunkFragmentEntity} is already covered by another {@link TupleChunkFragmentEntity}.
   */
  @Transactional
  public TupleChunkFragmentEntity keep(@NotNull TupleChunkFragmentEntity fragment)
      throws CastorClientException {
    checkNoConflict(fragment.getTupleChunkId(), fragment.getStartIndex(), fragment.getEndIndex());
    return fragmentRepository.save(fragment);
  }

  /**
   * Gets the {@link TupleChunkFragmentEntity} for the {@link TupleChunk} with the given id that
   * meets the following criteria:
   *
   * <ul>
   *   <li>is available for consumption having {@link TupleChunkFragmentEntity#activationStatus} set
   *       to {@link ActivationStatus#UNLOCKED}.
   *   <li>is not yet reserved to any other operation (having {@link
   *       TupleChunkFragmentEntity#reservationId} set to <code>null</code>.
   *   <li>contains the specified start index in the referenced tuple sequence.
   * </ul>
   *
   * @param tupleChunkId The id of the {@link TupleChunk} the {@link TupleChunkFragmentEntity}
   *     should reference.
   * @param startIndex The index of the tuple the {@link TupleChunkFragmentEntity} should reference.
   * @return Either an {@link Optional} containing the {@link TupleChunkFragmentEntity} that meets
   *     the described criteria or {@link Optional#empty()}.
   */
  @Transactional(readOnly = true)
  public Optional<TupleChunkFragmentEntity> findAvailableFragmentForChunkContainingIndex(
      UUID tupleChunkId, long startIndex) {
    return fragmentRepository.findAvailableFragmentForTupleChunkContainingIndex(
        tupleChunkId, startIndex);
  }

  /**
   * Gets a {@link TupleChunkFragmentEntity} that meets the following criteria:
   *
   * <ul>
   *   <li>is available for consumption having {@link TupleChunkFragmentEntity#activationStatus} set
   *       to {@link ActivationStatus#UNLOCKED}.
   *   <li>is not yet reserved to any other operation (having {@link
   *       TupleChunkFragmentEntity#reservationId} set to <code>null</code>.
   *   <li>references a chunk for the requested {@link TupleType}.
   * </ul>
   *
   * @param tupleType The requested type of tuples.
   * @return Either an {@link Optional} containing the {@link TupleChunkFragmentEntity} that meets
   *     the described criteria or {@link Optional#empty()}.
   */
  @Transactional(readOnly = true)
  public Optional<TupleChunkFragmentEntity> findAvailableFragmentWithTupleType(
      TupleType tupleType) {
    return fragmentRepository
        .findFirstByTupleTypeAndActivationStatusAndReservationIdNullOrderByIdAsc(
            tupleType, ActivationStatus.UNLOCKED);
  }

  /**
   * Reserve tuples as described by the given {@link Reservation}.
   *
   * @param reservation The {@link Reservation} to process.
   * @throws CastorServiceException if the tuples could not be reserved as requested.
   */
  @Transactional
  public void applyReservation(Reservation reservation) {
    log.debug("Apply reservation {}", reservation);
    for (ReservationElement re : reservation.getReservations()) {
      log.debug("Processing reservation element {}", re);
      long startIndex = re.getStartIndex();
      long endIndex = startIndex + re.getReservedTuples();
      while (startIndex < endIndex) {
        TupleChunkFragmentEntity fragment =
            fragmentRepository
                .findAvailableFragmentForTupleChunkContainingIndex(re.getTupleChunkId(), startIndex)
                .orElseThrow(
                    () ->
                        new CastorServiceException(
                            String.format(RE_CANNOT_BE_SATISFIED_EXCEPTION_FORMAT, reservation)));
        if (fragment.getStartIndex() < startIndex) {
          TupleChunkFragmentEntity nf =
              TupleChunkFragmentEntity.of(
                  fragment.getTupleChunkId(),
                  fragment.getTupleType(),
                  startIndex,
                  fragment.getEndIndex(),
                  fragment.getActivationStatus(),
                  fragment.getReservationId());
          fragment.setEndIndex(startIndex);
          fragmentRepository.save(fragment);
          fragment = nf;
        }
        if (endIndex < fragment.getEndIndex()) {
          TupleChunkFragmentEntity nf =
              TupleChunkFragmentEntity.of(
                  fragment.getTupleChunkId(),
                  fragment.getTupleType(),
                  endIndex,
                  fragment.getEndIndex(),
                  fragment.getActivationStatus(),
                  fragment.getReservationId());
          fragment.setEndIndex(endIndex);
          fragmentRepository.save(nf);
        }
        fragment.setReservationId(reservation.getReservationId());
        fragmentRepository.save(fragment);
        startIndex = fragment.getEndIndex();
      }
    }
  }

  /**
   * Verifies that a sequential section of tuple(s) within a {@link TupleChunk} is not yet described
   * by a {@link TupleChunkFragmentEntity}.
   *
   * @param chunkId The unique identifier of the referenced {@link TupleChunk}
   * @param startIndex The beginning of the tuple section to check
   * @param endIndex The end of the tuple section to check (<b>exclusive</b>)
   * @throws CastorClientException If at least one tuple in the given section is already referenced
   *     by a {@link TupleChunkFragmentEntity}
   */
  @Transactional(readOnly = true)
  public void checkNoConflict(UUID chunkId, long startIndex, long endIndex) {
    if (fragmentRepository
        .findFirstFragmentContainingAnyTupleOfSequence(chunkId, startIndex, endIndex)
        .isPresent()) {
      throw new CastorClientException(CONFLICT_EXCEPTION_MSG);
    }
  }

  /**
   * Returns the number of available tuples of a given {@link TupleType}.
   *
   * <p>Tuples referenced by {@link TupleChunkFragmentEntity fragments} that are either {@link
   * ActivationStatus#LOCKED} or reserved (see {@link TupleChunkFragmentEntity#reservationId} are
   * not counted.
   *
   * @param type T{@link TupleType} of interest.
   * @return the number of available tuples for the given type.
   */
  public long getAvailableTuples(TupleType type) {
    return fragmentRepository.getAvailableTupleByType(type);
  }

  /**
   * Activates all {@link TupleChunkFragmentEntity fragments} associated with the given tuple chunk
   * id.
   *
   * @param chunkId the unique identifier of the tuple chunk whose {@link TupleChunkFragmentEntity
   *     fragments} are to be activated.
   * @throws CastorServiceException if not a single {@link TupleChunkFragmentEntity} was associated
   *     with the given tuple chunk id
   */
  public void activateFragmentsForTupleChunk(UUID chunkId) {
    if (fragmentRepository.unlockAllForTupleChunk(chunkId) <= 0) {
      throw new CastorServiceException(NOT_A_SINGLE_FRAGMENT_FOR_CHUNK_ERROR_MSG);
    }
    ;
  }

  /**
   * Removes all {@link TupleChunkFragmentEntity fragments} associated with the given reservation
   * id.
   *
   * @param reservationId the unique identifier of the reservation.
   */
  public void deleteAllForReservationId(String reservationId) {
    fragmentRepository.deleteAllByReservationId(reservationId);
  }

  /**
   * Indicates whether any {@link TupleChunkFragmentEntity fragment} is associated with the given
   * tuple chunk id.
   *
   * @param tupleChunkId the unique identifier of the tuple chunk of interest.
   * @return <code>true</code> if any {@link TupleChunkFragmentEntity fragment} is associated with
   *     the given chunk id, <code>false</code> if not.
   */
  public boolean isChunkReferencedByFragments(UUID tupleChunkId) {
    return fragmentRepository.existsByTupleChunkId(tupleChunkId);
  }
}
