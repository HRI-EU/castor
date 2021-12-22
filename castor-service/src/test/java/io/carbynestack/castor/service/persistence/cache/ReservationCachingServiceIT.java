/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import static io.carbynestack.castor.common.entities.ActivationStatus.UNLOCKED;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import com.google.common.collect.Lists;
import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.service.CastorServiceApplication;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentRepository;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import io.carbynestack.castor.service.testconfig.PersistenceTestEnvironment;
import io.carbynestack.castor.service.testconfig.ReusableMinioContainer;
import io.carbynestack.castor.service.testconfig.ReusablePostgreSQLContainer;
import io.carbynestack.castor.service.testconfig.ReusableRedisContainer;
import io.carbynestack.castor.service.util.TupleChunkFragmentEntityListMatcher;
import java.util.*;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.cache.CacheKeyPrefix;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
    webEnvironment = RANDOM_PORT,
    classes = {CastorServiceApplication.class})
@ActiveProfiles("test")
public class ReservationCachingServiceIT {

  @ClassRule
  public static ReusableRedisContainer reusableRedisContainer =
      ReusableRedisContainer.getInstance();

  @ClassRule
  public static ReusableMinioContainer reusableMinioContainer =
      ReusableMinioContainer.getInstance();

  @ClassRule
  public static ReusablePostgreSQLContainer reusablePostgreSQLContainer =
      ReusablePostgreSQLContainer.getInstance();

  @Autowired private ConsumptionCachingService consumptionCachingService;

  @Autowired private PersistenceTestEnvironment testEnvironment;

  @Autowired private CastorCacheProperties cacheProperties;

  @Autowired private CacheManager cacheManager;

  @Autowired private RedisTemplate<String, Object> redisTemplate;

  @Autowired private TupleChunkFragmentRepository fragmentRepository;

  @Autowired private TupleChunkFragmentStorageService tupleChunkFragmentStorageService;

  private ReservationCachingService reservationCachingService;

  private Cache reservationCache;

  private final UUID testRequestId = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
  private final UUID testChunkId = UUID.fromString("80fbba1b-3da8-4b1e-8a2c-cebd65229fad");
  private final TupleType testTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
  private final String testReservationId = testRequestId + "_" + testTupleType;
  private final long testNumberReservedTuples = 3;
  private final Reservation testReservation =
      new Reservation(
          testReservationId,
          testTupleType,
          Collections.singletonList(
              new ReservationElement(testChunkId, testNumberReservedTuples, 0)));

  @Before
  public void setUp() {
    if (reservationCache == null) {
      reservationCache = cacheManager.getCache(cacheProperties.getReservationStore());
    }
    reservationCachingService =
        new ReservationCachingService(
            cacheProperties,
            consumptionCachingService,
            redisTemplate,
            tupleChunkFragmentStorageService);
    testEnvironment.clearAllData();
  }

  @Test
  public void givenCacheIsEmpty_whenGetReservation_thenReturnNull() {
    assertNull(reservationCachingService.getReservation(testReservation.getReservationId()));
  }

  @Test
  public void givenSuccessfulRequest_whenKeepReservation_thenStoreInCacheAndUpdateConsumption() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
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
    long requestedStartIndex = 42;
    long requestedLength = 21;
    ReservationElement reservationElement =
        new ReservationElement(tupleChunkId, requestedLength, requestedStartIndex);
    String reservationId = "testReservation";
    Reservation reservation =
        new Reservation(reservationId, tupleType, Collections.singletonList(reservationElement));

    existingFragment = fragmentRepository.save(existingFragment);

    reservationCachingService.keepReservation(reservation);

    assertEquals(reservation, reservationCache.get(reservationId, Reservation.class));
    Set<String> keysInCache =
        redisTemplate.keys(
            CacheKeyPrefix.simple()
                    .compute(
                        cacheProperties.getConsumptionStorePrefix()
                            + testReservation.getTupleType())
                + "*");
    assertEquals(1, keysInCache.size());
    assertEquals(
        requestedLength,
        consumptionCachingService.getConsumptionForTupleType(Long.MIN_VALUE, tupleType));

    List<TupleChunkFragmentEntity> actualFragments =
        Lists.newArrayList(fragmentRepository.findAll());
    assertEquals(3, actualFragments.size());
    MatcherAssert.assertThat(
        actualFragments,
        TupleChunkFragmentEntityListMatcher.containsAll(
            existingFragment.setEndIndex(requestedStartIndex),
            TupleChunkFragmentEntity.of(
                existingFragment.getTupleChunkId(),
                existingFragment.getTupleType(),
                requestedStartIndex,
                requestedStartIndex + requestedLength,
                UNLOCKED,
                reservationId),
            TupleChunkFragmentEntity.of(
                existingFragment.getTupleChunkId(),
                existingFragment.getTupleType(),
                requestedStartIndex + requestedLength,
                existingFragmentEndIndex,
                UNLOCKED,
                null)));
  }

  @Test
  public void givenReservationInCache_whenGetReservation_thenKeepReservationUntouchedInCache() {
    reservationCache.put(testReservation.getReservationId(), testReservation);
    assertEquals(
        testReservation,
        reservationCachingService.getReservation(testReservation.getReservationId()));
    assertEquals(
        testReservation,
        reservationCachingService.getReservation(testReservation.getReservationId()));
  }

  @Test
  public void givenSuccessfulRequest_whenForgetReservation_thenRemoveFromCache() {
    reservationCache.put(testReservation.getReservationId(), testReservation);
    assertEquals(
        testReservation,
        reservationCache.get(testReservation.getReservationId(), Reservation.class));
    reservationCachingService.forgetReservation(testReservation.getReservationId());
    assertNull(reservationCache.get(testReservation.getReservationId(), Reservation.class));
  }

  @Test
  public void whenReferencedSequenceIsSplitInFragments_whenApplyReservation_thenApplyAccordingly() {
    UUID tupleChunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long requestedStartIndex = 42;
    long requestedLength = 21;
    ReservationElement re =
        new ReservationElement(tupleChunkId, requestedLength, requestedStartIndex);
    String reservationId = "testReservation";
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    Reservation r = new Reservation(reservationId, tupleType, singletonList(re));

    TupleChunkFragmentEntity fragmentBefore =
        TupleChunkFragmentEntity.of(
            tupleChunkId, tupleType, 0, requestedStartIndex, UNLOCKED, null);
    TupleChunkFragmentEntity fragmentPart1 =
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            requestedStartIndex,
            requestedStartIndex + requestedLength - 5,
            UNLOCKED,
            null);
    TupleChunkFragmentEntity fragmentContainingRest =
        TupleChunkFragmentEntity.of(
            tupleChunkId,
            tupleType,
            requestedStartIndex + requestedLength - 5,
            Long.MAX_VALUE,
            UNLOCKED,
            null);

    fragmentBefore = fragmentRepository.save(fragmentBefore);
    fragmentPart1 = fragmentRepository.save(fragmentPart1);
    fragmentContainingRest = fragmentRepository.save(fragmentContainingRest);

    reservationCachingService.applyReservationRequiresTransaction(r);
    List<TupleChunkFragmentEntity> actualFragments =
        Lists.newArrayList(fragmentRepository.findAll());
    // fragment remains unchanged since it does not hold any tuples of interest
    assertTrue(actualFragments.remove(fragmentBefore));
    // fragment which is of interest as it is gets reserved
    assertTrue(actualFragments.remove(fragmentPart1.setReservationId(reservationId)));
    // fragment which contains some tuples of interest gets split and first part is reserved
    assertTrue(
        actualFragments.remove(
            fragmentContainingRest
                .setEndIndex(requestedStartIndex + requestedLength)
                .setReservationId(reservationId)));
    // remnant of previous fragment got created and is available
    assertEquals(1, actualFragments.size());
    TupleChunkFragmentEntity lastFragment = actualFragments.get(0);
    assertEquals(tupleChunkId, lastFragment.getTupleChunkId());
    assertEquals(tupleType, lastFragment.getTupleType());
    assertEquals(Long.MAX_VALUE, lastFragment.getEndIndex());
    assertEquals(requestedStartIndex + requestedLength, lastFragment.getStartIndex());
    assertEquals(UNLOCKED, lastFragment.getActivationStatus());
    assertNull(lastFragment.getReservationId());
  }
}
