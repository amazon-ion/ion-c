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

#include <tuple>
#include "ion_assert.h"
#include "ion_helpers.h"
#include "ion_event_util.h"
#include "ion_test_util.h"
#include "ion_event_equivalence.h"

class IonTimestamp : public ::testing::TestWithParam< ::testing::tuple<std::string, int, time_t, std::string> > {
public:
    std::string str;
    int local_offset;
    time_t time;
    std::string str_unknown_offset;

    virtual void SetUp() {
        str = ::testing::get<0>(GetParam());
        local_offset = ::testing::get<1>(GetParam());
        time = ::testing::get<2>(GetParam());
        str_unknown_offset = ::testing::get<3>(GetParam());
    }
};

INSTANTIATE_TEST_CASE_P(IonTimestamp, IonTimestamp, testing::Values(
        std::tr1::make_tuple(std::string("2020-07-01T14:24:57-01:00"), -60, 1593617097, std::string("2020-07-01T15:24:57-00:00")),
        std::tr1::make_tuple(std::string("2020-07-01T14:24:57+00:00"),   0, 1593613497, std::string("2020-07-01T14:24:57-00:00")),
        std::tr1::make_tuple(std::string("2020-07-01T14:24:57+01:00"),  60, 1593609897, std::string("2020-07-01T13:24:57-00:00"))
));

// regression for https://github.com/amzn/ion-c/issues/144
TEST_P(IonTimestamp, ion_timestamp_to_time_t) {
    ION_TIMESTAMP timestamp;
    SIZE chars_used;
    time_t actual_time;

    ION_ASSERT_OK(ion_timestamp_parse(&timestamp, (char *)str.c_str(), (SIZE)strlen(str.c_str()), &chars_used, &g_IonEventDecimalContext));
    ION_ASSERT_OK(ion_timestamp_to_time_t(&timestamp, &actual_time));
    ASSERT_EQ(time, actual_time);
}

TEST_P(IonTimestamp, ion_timestamp_for_time_t) {
    ION_TIMESTAMP timestamp;
    SIZE chars_used;

    ION_ASSERT_OK(ion_timestamp_for_time_t(&timestamp, &time));
    char to_string[ION_TIMESTAMP_STRING_LENGTH+1];
    ION_ASSERT_OK(ion_timestamp_to_string(&timestamp, (char *)to_string, (SIZE)sizeof(to_string), &chars_used, &g_IonEventDecimalContext));
    to_string[chars_used] = '\0';

    ASSERT_STREQ(str_unknown_offset.c_str(), to_string);
}

TEST_P(IonTimestamp, has_and_get_local_offset) {
    ION_TIMESTAMP timestamp;
    SIZE chars_used;
    int offset;
    BOOL has_local_offset;

    ION_ASSERT_OK(ion_timestamp_parse(&timestamp, (char *)str.c_str(), (SIZE)strlen(str.c_str()), &chars_used, &g_IonEventDecimalContext));
    ION_ASSERT_OK(ion_timestamp_has_local_offset(&timestamp, &has_local_offset));
    ASSERT_TRUE(has_local_offset);

    ION_ASSERT_OK(ion_timestamp_get_local_offset(&timestamp, &offset));
    ASSERT_EQ(local_offset, offset);
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

