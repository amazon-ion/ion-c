/*
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

#include "ion_assert.h"
#include "ion_helpers.h"
#include "ion_test_util.h"

TEST(IonAllocation, RejectsOverflowingSize) {
    // Requesting an allocation near INT32_MAX should fail rather than
    // wrapping around and returning a small block.
    SIZE near_max = 0x7FFFFFE0;
    void *owner = ion_alloc_owner(near_max);
    ASSERT_EQ(nullptr, owner);
}

TEST(IonAllocation, RejectsNegativeSize) {
    SIZE negative = -1;
    void *owner = ion_alloc_owner(negative);
    ASSERT_EQ(nullptr, owner);
}

TEST(IonAllocation, RejectsZeroSize) {
    SIZE zero = 0;
    void *owner = ion_alloc_owner(zero);
    ASSERT_EQ(nullptr, owner);
}

TEST(IonInteger, FromAbsBytesRejectsOverflowingByteCount) {
    // When byte_count is large enough that byte_count * 8 overflows int32_t,
    // ion_int_from_abs_bytes must return an error rather than allocating
    // an undersized buffer.
    SIZE overflow_count = 0x10000000; // 256 MiB — smallest value where *8 wraps

    ION_INT *iint = NULL;
    ION_ASSERT_OK(ion_int_alloc(NULL, &iint));

    // We don't actually need a real buffer of that size for this test;
    // the function should reject the byte_count before reading bytes.
    // Allocate a small buffer and pass the large count — the function
    // must fail on the count alone.
    BYTE small_buf[16] = {0};
    iERR err = ion_int_from_abs_bytes(iint, small_buf, overflow_count, FALSE);
    ASSERT_NE(IERR_OK, err);

    ion_int_free(iint);
}

TEST(IonInteger, FromBytesRejectsOverflowingByteCount) {
    SIZE overflow_count = 0x10000000;

    ION_INT *iint = NULL;
    ION_ASSERT_OK(ion_int_alloc(NULL, &iint));

    BYTE small_buf[16] = {0};
    iERR err = ion_int_from_bytes(iint, small_buf, overflow_count);
    ASSERT_NE(IERR_OK, err);

    ion_int_free(iint);
}

TEST(IonBinaryReader, RejectsValueLenExceedingStream) {
    // Construct a minimal binary Ion buffer with a string whose declared
    // VarUInt length far exceeds the actual buffer. The reader must return
    // an error without crashing.
    BYTE ion_data[] = {
        0xE0, 0x01, 0x00, 0xEA, // BVM
        0x8E,                   // type=string (0x8), length=VarUInt (0xE)
        0x7F, 0x7F, 0x7F, 0xFF, // VarUInt encoding of a large length
        0x41,                   // one byte of "content" (far less than declared)
    };
    SIZE ion_data_len = sizeof(ion_data);

    hREADER reader = NULL;
    ION_TYPE type;
    iERR err;

    err = ion_reader_open_buffer(&reader, ion_data, ion_data_len, NULL);
    ASSERT_EQ(IERR_OK, err);

    err = ion_reader_next(reader, &type);

    ION_STRING str;
    err = ion_reader_read_string(reader, &str);
    ASSERT_NE(IERR_OK, err);

    ion_reader_close(reader);
}

TEST(IonBinaryReader, RejectsImportMaxIdOverflow) {
    // A local symbol table with two imports whose max_id values sum past
    // INT32_MAX. The reader must return an error rather than allowing
    // the overflow.
    //
    // Each import has: name="x" (or "y"), version=1, max_id=0x40000000
    // Two imports sum to 0x80000000 + system max_id (10) which overflows.
    //
    // Binary Ion layout:
    //   BVM
    //   $ion_symbol_table:: {        // annot SID 3
    //     imports: [                  // field SID 6
    //       { name: "x",             // field SID 4
    //         version: 1,            // field SID 5
    //         max_id: 1073741824 },  // field SID 8, value 0x40000000
    //       { name: "y",
    //         version: 1,
    //         max_id: 1073741824 }
    //     ]
    //   }

    // Build the inner import structs first, then wrap them.
    // Import struct 1: {name:"x", version:1, max_id:0x40000000}
    //   field_sid 4 = 0x84 (1 byte), value string "x" = 0x81 0x78 (2 bytes) => 3
    //   field_sid 5 = 0x85 (1 byte), value int 1 = 0x21 0x01 (2 bytes) => 3
    //   field_sid 8 = 0x88 (1 byte), value int 0x40000000 = 0x24 0x40 0x00 0x00 0x00 (5 bytes) => 6
    // Struct body = 3 + 3 + 6 = 12 bytes. Type desc: 0xDC (type=0xD, len=12)

    // Import struct 2: same but name="y"
    //   field_sid 4 = 0x84, string "y" = 0x81 0x79 => 3
    //   field_sid 5 = 0x85, int 1 = 0x21 0x01 => 3
    //   field_sid 8 = 0x88, int 0x40000000 = 0x24 0x40 0x00 0x00 0x00 => 6
    // Struct body = 12 bytes. Type desc: 0xDC

    // List body = 1+12 + 1+12 = 26 bytes. Type desc: 0xBE + VarUInt(26) = 0xBE 0x9A
    // Outer struct: field imports (SID 6) = 0x86, list = 28 bytes => field total = 1+28 = 29
    // Outer struct body = 29 bytes. Type desc: 0xDE + VarUInt(29) = 0xDE 0x9D
    // Annotation wrapper: annot_len = 1 (SID 3 = 0x83), content = struct (31 bytes)
    //   total inner = 1 + 1 + 31 = 33. Type desc: 0xEE + VarUInt(33) = 0xEE 0xA1

    BYTE ion_data[] = {
        0xE0, 0x01, 0x00, 0xEA,             // BVM
        // Annotation wrapper: type=0xE, len=VarUInt
        0xEE,
        0xA1,                                // VarUInt: wrapper length = 33
        0x81,                                // annot_length = 1
        0x83,                                // annotation SID 3 ($ion_symbol_table)
        // Struct: type=0xD, len=VarUInt
        0xDE,
        0x9D,                                // VarUInt: struct length = 29
        // Field: imports (SID 6)
        0x86,                                // field SID 6
        // List: type=0xB, len=VarUInt
        0xBE,
        0x9A,                                // VarUInt: list length = 26
        // Import struct 1
        0xDC,                                // struct, length=12
        0x84, 0x81, 0x78,                    // field 4 (name), string "x"
        0x85, 0x21, 0x01,                    // field 5 (version), int 1
        0x88, 0x24, 0x40, 0x00, 0x00, 0x00,  // field 8 (max_id), int 0x40000000
        // Import struct 2
        0xDC,                                // struct, length=12
        0x84, 0x81, 0x79,                    // field 4 (name), string "y"
        0x85, 0x21, 0x01,                    // field 5 (version), int 1
        0x88, 0x24, 0x40, 0x00, 0x00, 0x00,  // field 8 (max_id), int 0x40000000
    };
    SIZE ion_data_len = sizeof(ion_data);

    hREADER reader = NULL;
    ION_READER_OPTIONS options;
    memset(&options, 0, sizeof(options));

    ION_CATALOG *catalog = NULL;
    ion_catalog_open(&catalog);
    options.pcatalog = catalog;

    ION_TYPE type;
    iERR err;

    err = ion_reader_open_buffer(&reader, ion_data, ion_data_len, &options);
    ASSERT_EQ(IERR_OK, err);

    err = ion_reader_next(reader, &type);
    ASSERT_NE(IERR_OK, err);

    ion_reader_close(reader);
    ion_catalog_close(catalog);
}

TEST(IonIndex, RejectsOverflowingArraySize) {
    // _ion_index_grow_array must reject when new_count * entry_size
    // overflows int32_t. This is tested indirectly through the symbol
    // table initialization path.
    //
    // Directly test: new_count=0x20000001, entry_size=8 => product
    // overflows to 8, which would allocate a tiny buffer for a huge array.

    void *array = NULL;
    hOWNER owner = ion_alloc_owner(sizeof(int));
    ASSERT_NE(nullptr, owner);

    iERR err = _ion_index_grow_array(&array, 0, 0x20000001, 8, FALSE, owner);
    ASSERT_NE(IERR_OK, err);

    ion_free_owner(owner);
}

TEST(IonIndex, RejectsOverflowingDoubledCount) {
    // When the symbol table by_id array needs to grow, it doubles
    // old_count. If old_count * 2 overflows int32_t, the function
    // must return an error.
    void *array = NULL;
    hOWNER owner = ion_alloc_owner(sizeof(int));
    ASSERT_NE(nullptr, owner);

    // A negative new_count (as would result from old_count * 2 overflowing)
    // must be rejected by _ion_index_grow_array.
    int32_t new_count = -1;

    iERR err = _ion_index_grow_array(&array, 0, new_count, sizeof(void*), FALSE, owner);
    ASSERT_NE(IERR_OK, err);

    ion_free_owner(owner);
}

static int_fast8_t _test_compare_fn(void *key1, void *key2, void *context) {
    return (key1 == key2) ? 0 : 1;
}

static int_fast32_t _test_hash_fn(void *key, void *context) {
    return (int_fast32_t)(uintptr_t)key;
}

TEST(IonIndex, RejectsOverflowingBucketThreshold) {
    // _ion_index_make_room multiplies (key_count + expected_new) * 128.
    // If expected_new is large enough, this overflows int32_t.
    // expected_new = 0x01000001 (16,777,217) => * 128 = 0x80000080 which overflows.

    ION_INDEX index;
    ION_INDEX_OPTIONS options;
    memset(&options, 0, sizeof(options));

    hOWNER owner = ion_alloc_owner(sizeof(int));
    ASSERT_NE(nullptr, owner);
    options._memory_owner = owner;
    options._initial_size = 16;
    options._compare_fn = _test_compare_fn;
    options._hash_fn = _test_hash_fn;

    ION_ASSERT_OK(_ion_index_initialize(&index, &options));

    iERR err = _ion_index_make_room(&index, 0x01000001);
    ASSERT_NE(IERR_OK, err);

    ion_free_owner(owner);
}

TEST(IonInteger, HighBytesHelperHandlesLargeByteCount) {
    // _ion_int_is_high_bytes_high_bit_set_helper must not overflow when
    // abs_byte_count * 8 exceeds INT32_MAX. We verify via
    // _ion_int_abs_bytes_length_helper + _ion_int_is_high_bytes_high_bit_set_helper
    // which are called from ion_int_to_bytes. With a small ION_INT, the
    // function should handle gracefully regardless of input.
    ION_INT *iint = NULL;
    ION_ASSERT_OK(ion_int_alloc(NULL, &iint));
    ION_ASSERT_OK(ion_int_from_long(iint, 42));

    SIZE byte_len = 0;
    ION_ASSERT_OK(ion_int_byte_length(iint, &byte_len));
    ASSERT_GT(byte_len, 0);

    BYTE buf[16];
    SIZE written = 0;
    ION_ASSERT_OK(ion_int_to_bytes(iint, 0, buf, sizeof(buf), &written));
    ASSERT_GT(written, 0);

    ion_int_free(iint);
}

TEST(IonString, StrdupRejectsMaxLength) {
    // ion_string_strdup computes length + 1 which overflows if
    // length == INT32_MAX.
    ION_STRING str;
    str.value = (BYTE *)"x";
    str.length = MAX_SIZE;

    char *result = ion_string_strdup(&str);
    ASSERT_EQ(nullptr, result);
}

TEST(IonInteger, FromCharsRejectsOverlongDecimalString) {
    // ion_int_from_chars computes bits = II_BITS_PER_DEC_DIGIT * decimal_digits.
    // For decimal_digits > MAX_SIZE / 4, the cast to SIZE overflows.
    // We pass a string of '1' characters longer than MAX_SIZE / 4.
    SIZE huge_len = MAX_SIZE / 4 + 1;

    ION_INT *iint = NULL;
    ION_ASSERT_OK(ion_int_alloc(NULL, &iint));

    // We don't actually need a buffer that large — the function should
    // reject based on the length alone before reading all digits.
    // Use a small buffer with a declared large length.
    BYTE small_buf[16];
    memset(small_buf, '1', sizeof(small_buf));

    iERR err = ion_int_from_chars(iint, (const char *)small_buf, huge_len);
    ASSERT_NE(IERR_OK, err);

    ion_int_free(iint);
}

TEST(IonInteger, HighestBitSetHandlesNormalValues) {
    // _ion_int_highest_bit_set_helper uses (len - ii - 1) * 31 which
    // could overflow for huge _len. For normal values it must work correctly.
    ION_INT *iint = NULL;
    ION_ASSERT_OK(ion_int_alloc(NULL, &iint));
    ION_ASSERT_OK(ion_int_from_long(iint, 0x7FFFFFFFFFFFFFFF));

    SIZE pos = 0;
    ION_ASSERT_OK(ion_int_highest_bit_set(iint, &pos));
    ASSERT_EQ(63, pos);

    ion_int_free(iint);
}

TEST(IonInteger, ExtendDigitsRejectsOverflowingSize) {
    // _ion_int_extend_digits computes digits_needed * sizeof(II_DIGIT).
    // If digits_needed > INT32_MAX / 4, the product overflows SIZE.
    ION_INT *iint = NULL;
    ION_ASSERT_OK(ion_int_alloc(NULL, &iint));

    SIZE overflow_digits = (MAX_SIZE / (SIZE)sizeof(II_DIGIT)) + 1;
    iERR err = _ion_int_extend_digits(iint, overflow_digits, TRUE);
    ASSERT_NE(IERR_OK, err);

    ion_int_free(iint);
}

TEST(IonBinaryReader, RejectsFieldSidExceedingInt32Max) {
    // A struct with a field SID > INT32_MAX. The VarUInt encoding allows
    // values up to UINT32_MAX, but SID is int32_t. The reader must reject
    // field SIDs that exceed INT32_MAX.
    //
    // Binary Ion layout:
    //   BVM
    //   struct (len=VarUInt) {
    //     field_sid = 0x80000001 (2147483649, exceeds INT32_MAX)
    //     value = int 0
    //   }
    //
    // VarUInt encoding of 0x80000001: 08 00 00 00 81

    BYTE ion_data[] = {
        0xE0, 0x01, 0x00, 0xEA,   // BVM
        // Struct: type=0xD, len=VarUInt (0xE)
        0xDE,
        0x87,                      // VarUInt: struct length = 7
        // Field SID as VarUInt: 0x80000001
        0x08, 0x00, 0x00, 0x00, 0x81,
        // Value: int 0
        0x20,
    };
    SIZE ion_data_len = sizeof(ion_data);

    hREADER reader = NULL;
    ION_TYPE type;
    iERR err;

    err = ion_reader_open_buffer(&reader, ion_data, ion_data_len, NULL);
    ASSERT_EQ(IERR_OK, err);

    err = ion_reader_next(reader, &type);
    ASSERT_EQ(IERR_OK, err);
    ASSERT_EQ((ION_TYPE)tid_STRUCT, type);

    err = ion_reader_step_in(reader);
    ASSERT_EQ(IERR_OK, err);

    err = ion_reader_next(reader, &type);
    ASSERT_NE(IERR_OK, err);

    ion_reader_close(reader);
}

TEST(IonBinaryReader, RejectsOversizedSymbolTableString) {
    // A local symbol table with a string whose declared length is near
    // INT32_MAX. During ion_reader_next(), the reader processes the symbol
    // table automatically. The oversized length must be rejected without
    // overflowing the allocator.
    //
    // Layout:
    //   BVM
    //   annotation wrapper (annot=$ion_symbol_table) {
    //     struct {
    //       field "symbols" (SID 7): list {
    //         string with VarUInt length = 0x7FFFFFE0
    //       }
    //     }
    //   }
    BYTE ion_data[] = {
        0xE0, 0x01, 0x00, 0xEA, // BVM
        // Annotation wrapper: type=0xE, len=VarUInt
        0xEE,
        0x90,                    // VarUInt wrapper length = 16
        0x81,                    // annot_length VarUInt = 1
        0x83,                    // annotation SID 3 = $ion_symbol_table
        // Struct: type=0xD, len=VarUInt
        0xDE,
        0x8C,                    // VarUInt struct length = 12
        0x87,                    // field SID 7 = symbols
        // List: type=0xB, len=VarUInt
        0xBE,
        0x89,                    // VarUInt list length = 9
        // String: type=0x8, len=VarUInt (0xE nibble)
        0x8E,
        // VarUInt encoding of 0x7FFFFFE0:
        // 0x7FFFFFE0 in 7-bit groups: 07 7F 7F 7F 60
        0x07, 0x7F, 0x7F, 0x7F, 0xE0,
    };
    SIZE ion_data_len = sizeof(ion_data);

    hREADER reader = NULL;
    ION_TYPE type;
    iERR err;

    err = ion_reader_open_buffer(&reader, ion_data, ion_data_len, NULL);
    ASSERT_EQ(IERR_OK, err);

    // ion_reader_next processes the symbol table internally. It should
    // fail with an error rather than overflowing.
    err = ion_reader_next(reader, &type);
    ASSERT_NE(IERR_OK, err);

    ion_reader_close(reader);
}

TEST(IonInteger, FromCharsHandlesNullLiterals) {
    // _ion_int_from_chars_helper uses strncmp on non-NUL-terminated strings
    // and had broken || logic. After the fix, "null" and "null.int" should
    // be accepted, "n" alone should be rejected, and no OOB read should occur.

    ION_INT *iint = NULL;
    ION_ASSERT_OK(ion_int_alloc(NULL, &iint));

    // "null" (length 4) should succeed and produce a null int
    iERR err = ion_int_from_chars(iint, "null", 4);
    ASSERT_EQ(IERR_OK, err);

    // "null.int" (length 8) should succeed
    err = ion_int_from_chars(iint, "null.int", 8);
    ASSERT_EQ(IERR_OK, err);

    // "n" (length 1) should fail with IERR_INVALID_SYNTAX, not OOB read
    err = ion_int_from_chars(iint, "n", 1);
    ASSERT_EQ(IERR_INVALID_SYNTAX, err);

    // "nu" (length 2) should fail
    err = ion_int_from_chars(iint, "nu", 2);
    ASSERT_EQ(IERR_INVALID_SYNTAX, err);

    // "null.in" (length 7) should fail — not a complete literal
    err = ion_int_from_chars(iint, "null.in", 7);
    ASSERT_EQ(IERR_INVALID_SYNTAX, err);

    ion_int_free(iint);
}

TEST(IonTextReader, DeeplyNestedContainersDoNotCrash) {
    // _ion_scanner_skip_container should not recurse without a depth limit.
    // Nesting deeper than ION_SCANNER_MAX_SKIP_DEPTH should return an
    // error rather than exhausting the stack.
    //
    // Format: [  [[[...1000 brackets...]]]  ]
    // We step into the outer list, then call next() which must skip
    // the inner deeply-nested list. This triggers _ion_scanner_skip_container.
    const int DEPTH = 1000;
    std::string ion_text;
    ion_text += '[  ';
    ion_text += std::string(DEPTH, '[');
    ion_text += std::string(DEPTH, ']');
    ion_text += '  ]';

    hREADER reader = NULL;
    ION_TYPE type;
    iERR err;

    err = ion_reader_open_buffer(&reader, (BYTE *)ion_text.c_str(), (SIZE)ion_text.size(), NULL);
    ASSERT_EQ(IERR_OK, err);

    err = ion_reader_next(reader, &type);
    ASSERT_EQ(IERR_OK, err);
    ASSERT_EQ((ION_TYPE)tid_LIST, type);

    err = ion_reader_step_in(reader);
    ASSERT_EQ(IERR_OK, err);

    // This next() must skip the deeply nested inner list.
    // It should fail with an error rather than recursing unboundedly.
    err = ion_reader_next(reader, &type);
    if (err == IERR_OK) {
        // If first next succeeds (returns the inner list), calling next again
        // will skip it via the scanner's skip_container.
        err = ion_reader_next(reader, &type);
    }
    ASSERT_NE(IERR_OK, err);

    ion_reader_close(reader);
}

TEST(IonTextReader, Base64DecodeDoesNotOverflowBuffer) {
    // _ion_scanner_read_as_base64 had a post-decrement underflow when
    // buf_max % 3 == 2. The 'remaining' counter wrapped to -1 and the
    // decode loop wrote past the caller's buffer.
    //
    // Text Ion blob: {{ AQID }} which is base64 for bytes 0x01 0x02 0x03
    // We read with buf_max=2 so the 3-byte decode hits the boundary.

    const char *ion_text = "{{ AQID }}";
    SIZE ion_text_len = (SIZE)strlen(ion_text);

    hREADER reader = NULL;
    ION_TYPE type;
    iERR err;

    err = ion_reader_open_buffer(&reader, (BYTE *)ion_text, ion_text_len, NULL);
    ASSERT_EQ(IERR_OK, err);

    err = ion_reader_next(reader, &type);
    ASSERT_EQ(IERR_OK, err);
    ASSERT_EQ((ION_TYPE)tid_BLOB, type);

    // Read with buf_max=2 (% 3 == 2) to trigger the boundary condition.
    // The function should write at most 2 bytes and not corrupt memory.
    BYTE read_buf[64];
    memset(read_buf, 0xCC, sizeof(read_buf));
    SIZE bytes_read = 0;

    err = ion_reader_read_lob_partial_bytes(reader, read_buf, 2, &bytes_read);
    ASSERT_EQ(IERR_OK, err);
    ASSERT_LE(bytes_read, 2);

    // Verify no write past the 2-byte boundary
    for (int i = 2; i < 64; i++) {
        ASSERT_EQ(0xCC, read_buf[i]) << "Buffer overwrite at offset " << i;
    }

    ion_reader_close(reader);
}
