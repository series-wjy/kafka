<<<<<<< HEAD
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

=======
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
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
package org.apache.kafka.streams.kstream;

import org.junit.Test;

<<<<<<< HEAD
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


public class JoinWindowsTest {

    private static String anyName = "window";
    private static long anySize = 123L;
    private static long anyOtherSize = 456L; // should be larger than anySize

    @Test
    public void shouldHaveSaneEqualsAndHashCode() {
        JoinWindows w1 = JoinWindows.of("w1").with(anySize);
        JoinWindows w2 = JoinWindows.of("w2").with(anySize);

        // Reflexive
        assertEquals(w1, w1);
        assertEquals(w1.hashCode(), w1.hashCode());

        // Symmetric
        assertEquals(w1, w2);
        assertEquals(w2, w1);
        assertEquals(w1.hashCode(), w2.hashCode());

        JoinWindows w3 = JoinWindows.of("w3").with(w2.after).before(anyOtherSize);
        JoinWindows w4 = JoinWindows.of("w4").with(anyOtherSize).after(w2.after);
        assertEquals(w3, w4);
        assertEquals(w4, w3);
        assertEquals(w3.hashCode(), w4.hashCode());

        // Inequality scenarios
        assertNotEquals("must be false for null", null, w1);
        assertNotEquals("must be false for different window types", UnlimitedWindows.of("irrelevant"), w1);
        assertNotEquals("must be false for different types", new Object(), w1);

        JoinWindows differentWindowSize = JoinWindows.of("differentWindowSize").with(w1.after + 1);
        assertNotEquals("must be false when window sizes are different", differentWindowSize, w1);

        JoinWindows differentWindowSize2 = JoinWindows.of("differentWindowSize").with(w1.after).after(w1.after + 1);
        assertNotEquals("must be false when window sizes are different", differentWindowSize2, w1);

        JoinWindows differentWindowSize3 = JoinWindows.of("differentWindowSize").with(w1.after).before(w1.before + 1);
        assertNotEquals("must be false when window sizes are different", differentWindowSize3, w1);
    }

    @Test
    public void validWindows() {
        JoinWindows.of(anyName)
            .with(anyOtherSize)     // [ -anyOtherSize ; anyOtherSize ]
            .before(anySize)        // [ -anySize ; anyOtherSize ]
            .before(0)              // [ 0 ; anyOtherSize ]
            .before(-anySize)       // [ anySize ; anyOtherSize ]
            .before(-anyOtherSize); // [ anyOtherSize ; anyOtherSize ]

        JoinWindows.of(anyName)
            .with(anyOtherSize)     // [ -anyOtherSize ; anyOtherSize ]
            .after(anySize)         // [ -anyOtherSize ; anySize ]
            .after(0)               // [ -anyOtherSize ; 0 ]
            .after(-anySize)        // [ -anyOtherSize ; -anySize ]
            .after(-anyOtherSize);  // [ -anyOtherSize ; -anyOtherSize ]
    }

    @Test(expected = IllegalArgumentException.class)
    public void nameMustNotBeEmpty() {
        JoinWindows.of("").with(anySize);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nameMustNotBeNull() {
        JoinWindows.of(null).with(anySize);
    }

    @Test(expected = IllegalArgumentException.class)
    public void timeDifferenceMustNotBeNegative() {
        JoinWindows.of(anyName).with(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void afterBelowLower() {
        JoinWindows.of(anyName).with(anySize).after(-anySize - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void beforeOverUpper() {
        JoinWindows.of(anyName).with(anySize).before(-anySize - 1);
    }

}
=======
import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class JoinWindowsTest {

    private static final long ANY_SIZE = 123L;
    private static final long ANY_OTHER_SIZE = 456L; // should be larger than anySize

    @Test
    public void validWindows() {
        JoinWindows.of(ofMillis(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
                   .before(ofMillis(ANY_SIZE))                    // [ -anySize ; anyOtherSize ]
                   .before(ofMillis(0))                          // [ 0 ; anyOtherSize ]
                   .before(ofMillis(-ANY_SIZE))                   // [ anySize ; anyOtherSize ]
                   .before(ofMillis(-ANY_OTHER_SIZE));             // [ anyOtherSize ; anyOtherSize ]

        JoinWindows.of(ofMillis(ANY_OTHER_SIZE))   // [ -anyOtherSize ; anyOtherSize ]
                   .after(ofMillis(ANY_SIZE))                     // [ -anyOtherSize ; anySize ]
                   .after(ofMillis(0))                           // [ -anyOtherSize ; 0 ]
                   .after(ofMillis(-ANY_SIZE))                    // [ -anyOtherSize ; -anySize ]
                   .after(ofMillis(-ANY_OTHER_SIZE));              // [ -anyOtherSize ; -anyOtherSize ]
    }

    @Test(expected = IllegalArgumentException.class)
    public void timeDifferenceMustNotBeNegative() {
        JoinWindows.of(ofMillis(-1));
    }

    @Test
    public void endTimeShouldNotBeBeforeStart() {
        final JoinWindows windowSpec = JoinWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.after(ofMillis(-ANY_SIZE - 1));
            fail("window end time should not be before window start time");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void startTimeShouldNotBeAfterEnd() {
        final JoinWindows windowSpec = JoinWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.before(ofMillis(-ANY_SIZE - 1));
            fail("window start time should not be after window end time");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void untilShouldSetGraceDuration() {
        final JoinWindows windowSpec = JoinWindows.of(ofMillis(ANY_SIZE));
        final long windowSize = windowSpec.size();
        assertEquals(windowSize, windowSpec.grace(ofMillis(windowSize)).gracePeriodMs());
    }

    @Deprecated
    @Test
    public void retentionTimeMustNoBeSmallerThanWindowSize() {
        final JoinWindows windowSpec = JoinWindows.of(ofMillis(ANY_SIZE));
        final long windowSize = windowSpec.size();
        try {
            windowSpec.until(windowSize - 1);
            fail("should not accept retention time smaller than window size");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void gracePeriodShouldEnforceBoundaries() {
        JoinWindows.of(ofMillis(3L)).grace(ofMillis(0L));

        try {
            JoinWindows.of(ofMillis(3L)).grace(ofMillis(-1L));
            fail("should not accept negatives");
        } catch (final IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        verifyEquality(JoinWindows.of(ofMillis(3)), JoinWindows.of(ofMillis(3)));

        verifyEquality(JoinWindows.of(ofMillis(3)).after(ofMillis(2)), JoinWindows.of(ofMillis(3)).after(ofMillis(2)));

        verifyEquality(JoinWindows.of(ofMillis(3)).before(ofMillis(2)), JoinWindows.of(ofMillis(3)).before(ofMillis(2)));

        verifyEquality(JoinWindows.of(ofMillis(3)).grace(ofMillis(2)), JoinWindows.of(ofMillis(3)).grace(ofMillis(2)));

        verifyEquality(JoinWindows.of(ofMillis(3)).grace(ofMillis(60)), JoinWindows.of(ofMillis(3)).grace(ofMillis(60)));

        verifyEquality(
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3)).grace(ofMillis(60)),
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3)).grace(ofMillis(60))
        );
        // JoinWindows is a little weird in that before and after set the same fields as of.
        verifyEquality(
            JoinWindows.of(ofMillis(9)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3)).grace(ofMillis(60)),
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3)).grace(ofMillis(60))
        );
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {
        verifyInEquality(JoinWindows.of(ofMillis(9)), JoinWindows.of(ofMillis(3)));

        verifyInEquality(JoinWindows.of(ofMillis(3)).after(ofMillis(9)), JoinWindows.of(ofMillis(3)).after(ofMillis(2)));

        verifyInEquality(JoinWindows.of(ofMillis(3)).before(ofMillis(9)), JoinWindows.of(ofMillis(3)).before(ofMillis(2)));

        verifyInEquality(JoinWindows.of(ofMillis(3)).grace(ofMillis(9)), JoinWindows.of(ofMillis(3)).grace(ofMillis(2)));

        verifyInEquality(JoinWindows.of(ofMillis(3)).grace(ofMillis(90)), JoinWindows.of(ofMillis(3)).grace(ofMillis(60)));


        verifyInEquality(
            JoinWindows.of(ofMillis(3)).before(ofMillis(9)).after(ofMillis(2)).grace(ofMillis(3)),
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3))
        );

        verifyInEquality(
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(9)).grace(ofMillis(3)),
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3))
        );

        verifyInEquality(
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(9)),
            JoinWindows.of(ofMillis(3)).before(ofMillis(1)).after(ofMillis(2)).grace(ofMillis(3))
        );
    }
}
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
