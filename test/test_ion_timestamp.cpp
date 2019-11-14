/*
 * Copyright 2009 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <err.h>
#include "ion_assert.h"
#include "ion_helpers.h"
#include "ion_event_util.h"
#include "ion_test_util.h"
#include "ion_event_equivalence.h"

struct timestamp_test {
    const char *str;
    int local_offset;
    time_t time;
    const char *str_unknown_offset;
};

const int num_tests = 3;
struct timestamp_test tests[num_tests] = {
        {"2020-07-01T14:24:57-01:00", -60, 1593617097, "2020-07-01T15:24:57-00:00"},
        {"2020-07-01T14:24:57+00:00",   0, 1593613497, "2020-07-01T14:24:57-00:00"},
        {"2020-07-01T14:24:57+01:00",  60, 1593609897, "2020-07-01T13:24:57-00:00"},
};

// regression for https://github.com/amzn/ion-c/issues/144
TEST(IonTimestamp, ion_timestamp_to_time_t) {
    struct timestamp_test *test = tests;
    for (int i = 0; i < num_tests; i++, test++ ) {
        ION_TIMESTAMP timestamp;
        SIZE chars_used;
        time_t time;

        ION_ASSERT_OK(ion_timestamp_parse(&timestamp, (char *)test->str, (SIZE)strlen(test->str), &chars_used, &g_IonEventDecimalContext));
        ION_ASSERT_OK(ion_timestamp_to_time_t(&timestamp, &time));
        ASSERT_EQ(test->time, time);
    }
}

TEST(IonTimestamp, ion_timestamp_for_time_t) {
    struct timestamp_test *test = tests;
    for (int i = 0; i < num_tests; i++, test++ ) {
        ION_TIMESTAMP timestamp;
        SIZE chars_used;

        ION_ASSERT_OK(ion_timestamp_for_time_t(&timestamp, &test->time));
        char str_unknown_offset[ION_TIMESTAMP_STRING_LENGTH+1];
        ION_ASSERT_OK(ion_timestamp_to_string(&timestamp, str_unknown_offset, (SIZE)sizeof(str_unknown_offset), &chars_used, &g_IonEventDecimalContext));
        str_unknown_offset[chars_used] = '\0';

        ASSERT_STREQ(test->str_unknown_offset, str_unknown_offset);
    }
}

TEST(IonTimestamp, has_and_get_local_offset) {
    struct timestamp_test *test = tests;
    for (int i = 0; i < num_tests; i++, test++ ) {
        ION_TIMESTAMP timestamp;
        SIZE chars_used;
        int local_offset;
        BOOL has_local_offset;

        ION_ASSERT_OK(ion_timestamp_parse(&timestamp, (char *)test->str, (SIZE)strlen(test->str), &chars_used, &g_IonEventDecimalContext));
        ION_ASSERT_OK(ion_timestamp_has_local_offset(&timestamp, &has_local_offset));
        ASSERT_TRUE(has_local_offset);

        ION_ASSERT_OK(ion_timestamp_get_local_offset(&timestamp, &local_offset));
        ASSERT_EQ(test->local_offset, local_offset);
    }
}

TEST(IonTimestamp, IgnoresSuperfluousOffset) {
    ION_TIMESTAMP expected1, expected2, actual;
    BOOL has_local_offset;
    int local_offset;

    ION_ASSERT_OK(ion_timestamp_for_year(&expected1, 1));

    ION_ASSERT_OK(ion_timestamp_for_year(&expected2, 1));
    SET_FLAG_ON(expected2.precision, ION_TT_BIT_TZ);
    expected2.tz_offset = 1;

    ION_ASSERT_OK(ion_timestamp_for_year(&actual, 1));
    ION_ASSERT_OK(ion_timestamp_set_local_offset(&actual, 1));
    ION_ASSERT_OK(ion_timestamp_has_local_offset(&actual, &has_local_offset));
    ION_ASSERT_OK(ion_timestamp_get_local_offset(&actual, &local_offset));

    ASSERT_FALSE(has_local_offset);
    ASSERT_EQ(0, actual.tz_offset);
    ASSERT_EQ(0, local_offset);
    ASSERT_TRUE(ion_equals_timestamp(&expected1, &actual));
    ASSERT_TRUE(ion_equals_timestamp(&expected2, &actual)); // Equivalence ignores the superfluous offset as well.
}

