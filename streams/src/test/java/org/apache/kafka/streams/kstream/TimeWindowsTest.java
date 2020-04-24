/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.Test;

import java.util.Map;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
<<<<<<< HEAD
=======
import static org.junit.Assert.fail;
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b

@SuppressWarnings("deprecation")
public class TimeWindowsTest {

    private static final long ANY_SIZE = 123L;

    @Test
<<<<<<< HEAD
    public void shouldHaveSaneEqualsAndHashCode() {
        TimeWindows w1 = TimeWindows.of("w1", anySize);
        TimeWindows w2 = TimeWindows.of("w2", w1.size);

        // Reflexive
        assertEquals(w1, w1);
        assertEquals(w1.hashCode(), w1.hashCode());

        // Symmetric
        assertEquals(w1, w2);
        assertEquals(w2, w1);
        assertEquals(w1.hashCode(), w2.hashCode());

        // Transitive
        TimeWindows w3 = TimeWindows.of("w3", w2.size);
        assertEquals(w2, w3);
        assertEquals(w1, w3);
        assertEquals(w1.hashCode(), w3.hashCode());

        // Inequality scenarios
        assertNotEquals("must be false for null", null, w1);
        assertNotEquals("must be false for different window types", UnlimitedWindows.of("irrelevant"), w1);
        assertNotEquals("must be false for different types", new Object(), w1);

        TimeWindows differentWindowSize = TimeWindows.of("differentWindowSize", w1.size + 1);
        assertNotEquals("must be false when window sizes are different", differentWindowSize, w1);

        TimeWindows differentAdvanceInterval = w1.advanceBy(w1.advance - 1);
        assertNotEquals("must be false when advance intervals are different", differentAdvanceInterval, w1);
=======
    public void shouldSetWindowSize() {
        assertEquals(ANY_SIZE, TimeWindows.of(ofMillis(ANY_SIZE)).sizeMs);
    }

    @Test
    public void shouldSetWindowAdvance() {
        final long anyAdvance = 4;
        assertEquals(anyAdvance, TimeWindows.of(ofMillis(ANY_SIZE)).advanceBy(ofMillis(anyAdvance)).advanceMs);
    }

    @SuppressWarnings("deprecation") // specifically testing deprecated APIs
    @Test
    public void shouldSetWindowRetentionTime() {
        assertEquals(ANY_SIZE, TimeWindows.of(ofMillis(ANY_SIZE)).until(ANY_SIZE).maintainMs());
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
    }

    @SuppressWarnings("deprecation") // specifically testing deprecated APIs
    @Test
    public void shouldUseWindowSizeAsRentitionTimeIfWindowSizeIsLargerThanDefaultRetentionTime() {
        final long windowSize = 2 * TimeWindows.of(ofMillis(1)).maintainMs();
        assertEquals(windowSize, TimeWindows.of(ofMillis(windowSize)).maintainMs());
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeZero() {
        TimeWindows.of(ofMillis(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeNegative() {
        TimeWindows.of(ofMillis(-1));
    }

    @Test
    public void advanceIntervalMustNotBeZero() {
        final TimeWindows windowSpec = TimeWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.advanceBy(ofMillis(0));
            fail("should not accept zero advance parameter");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void advanceIntervalMustNotBeNegative() {
        final TimeWindows windowSpec = TimeWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.advanceBy(ofMillis(-1));
            fail("should not accept negative advance parameter");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Deprecated
    @Test
    public void advanceIntervalMustNotBeLargerThanWindowSize() {
        final TimeWindows windowSpec = TimeWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.advanceBy(ofMillis(ANY_SIZE + 1));
            fail("should not accept advance greater than window size");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Deprecated
    @Test
    public void retentionTimeMustNoBeSmallerThanWindowSize() {
        final TimeWindows windowSpec = TimeWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.until(ANY_SIZE - 1);
            fail("should not accept retention time smaller than window size");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void gracePeriodShouldEnforceBoundaries() {
        TimeWindows.of(ofMillis(3L)).grace(ofMillis(0L));

        try {
            TimeWindows.of(ofMillis(3L)).grace(ofMillis(-1L));
            fail("should not accept negatives");
        } catch (final IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void shouldComputeWindowsForHoppingWindows() {
        final TimeWindows windows = TimeWindows.of(ofMillis(12L)).advanceBy(ofMillis(5L));
        final Map<Long, TimeWindow> matched = windows.windowsFor(21L);
        assertEquals(12L / 5L + 1, matched.size());
        assertEquals(new TimeWindow(10L, 22L), matched.get(10L));
        assertEquals(new TimeWindow(15L, 27L), matched.get(15L));
        assertEquals(new TimeWindow(20L, 32L), matched.get(20L));
    }

    @Test
<<<<<<< HEAD
    public void windowsForBarelyOverlappingHoppingWindows() {
        TimeWindows windows = TimeWindows.of(anyName, 6L).advanceBy(5L);
        Map<Long, TimeWindow> matched = windows.windowsFor(7L);
=======
    public void shouldComputeWindowsForBarelyOverlappingHoppingWindows() {
        final TimeWindows windows = TimeWindows.of(ofMillis(6L)).advanceBy(ofMillis(5L));
        final Map<Long, TimeWindow> matched = windows.windowsFor(7L);
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        assertEquals(1, matched.size());
        assertEquals(new TimeWindow(5L, 11L), matched.get(5L));
    }

    @Test
<<<<<<< HEAD
    public void windowsForTumblingWindows() {
        TimeWindows windows = TimeWindows.of(anyName, 12L);
        Map<Long, TimeWindow> matched = windows.windowsFor(21L);
=======
    public void shouldComputeWindowsForTumblingWindows() {
        final TimeWindows windows = TimeWindows.of(ofMillis(12L));
        final Map<Long, TimeWindow> matched = windows.windowsFor(21L);
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        assertEquals(1, matched.size());
        assertEquals(new TimeWindow(12L, 24L), matched.get(12L));
    }


    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        verifyEquality(TimeWindows.of(ofMillis(3)), TimeWindows.of(ofMillis(3)));

        verifyEquality(TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)), TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)));

        verifyEquality(TimeWindows.of(ofMillis(3)).grace(ofMillis(1)), TimeWindows.of(ofMillis(3)).grace(ofMillis(1)));

        verifyEquality(TimeWindows.of(ofMillis(3)).grace(ofMillis(4)), TimeWindows.of(ofMillis(3)).grace(ofMillis(4)));

        verifyEquality(
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)).grace(ofMillis(1)).grace(ofMillis(4)),
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)).grace(ofMillis(1)).grace(ofMillis(4))
        );
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {
        verifyInEquality(TimeWindows.of(ofMillis(9)), TimeWindows.of(ofMillis(3)));

        verifyInEquality(TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(2)), TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)));

        verifyInEquality(TimeWindows.of(ofMillis(3)).grace(ofMillis(2)), TimeWindows.of(ofMillis(3)).grace(ofMillis(1)));

        verifyInEquality(TimeWindows.of(ofMillis(3)).grace(ofMillis(9)), TimeWindows.of(ofMillis(3)).grace(ofMillis(4)));


        verifyInEquality(
            TimeWindows.of(ofMillis(4)).advanceBy(ofMillis(2)).grace(ofMillis(2)),
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(2)).grace(ofMillis(2))
        );

        verifyInEquality(
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)).grace(ofMillis(2)),
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(2)).grace(ofMillis(2))
        );

        assertNotEquals(
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(2)).grace(ofMillis(1)),
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(2)).grace(ofMillis(2))
        );
    }
}
