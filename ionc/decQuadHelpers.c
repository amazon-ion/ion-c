/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

// helper functions

#include "ion_internal.h"
#include "ion_decimal_impl.h"
#include <math.h>

// Max uint64_t is 18446744073709551615; decimal digits are retrieved 9 at a time.
#define UINT64_TOP_2_DIGITS (uint64_t)18
#define UINT64_BOTTOM_18_DIGITS (uint64_t)446744073709551615L

// is this really faster than 4 if's?
static int dec_quad_helper_shift_table_for_nibbles[] = {
     0 // 0000
    ,1 // 0001
    ,2 // 0010
    ,2 // 0011

    ,3 // 0100
    ,3 // 0101
    ,3 // 0110
    ,3 // 0111

    ,4 // 1000
    ,4 // 1001
    ,4 // 1010
    ,4 // 1011

    ,4 // 1100
    ,4 // 1101
    ,4 // 1110
    ,4 // 1111
};


void ion_quad_get_exponent_and_shift(const decQuad *quad_value, decContext *set,
                                     decQuad *p_int_mantissa, int32_t *p_exp)
{
    decQuad scale;

    *p_exp = decQuadGetExponent(quad_value);

    // TODO is it possible to normalize some large mantissas so that they can be compressed?
    // Consider 40<three-hundred zeros>. Is this data-model equivalent to 4e301 ? If so, it could be written in its
    // compact form.
    if (*p_exp == 0) {
        decQuadCopy(p_int_mantissa, quad_value);
    }
    else {
        decQuadFromInt32(&scale, -*p_exp);
        decQuadScaleB(p_int_mantissa, quad_value, &scale, set);
        ASSERT(decQuadIsInteger(p_int_mantissa)); // The exponent should have been shifted out completely.
    }
}

iERR ion_quad_get_quad_from_digits_and_exponent(uint64_t value, int32_t exp,
        decContext *set, BOOL is_negative, decQuad *p_quad)
{
    iENTER;
    decQuad result, nine_quad_digits;
    decQuad multiplier;
    int32_t nine_digits;
    int     multiplier_exponent;
    uint32_t saved_status;

    // decDoubleScaleB(r, x, y, set) - This calculates x * 10y and places
    //                                 the result in r.

    decQuadZero(&result);

    ION_DECIMAL_SAVE_STATUS(saved_status, set, DEC_Inexact);

    decQuadFromInt32(&multiplier, 1);
    multiplier_exponent = 0;
    while (value > 0) {
        // crack off the lower 9 digits (and removed them from the original)
        nine_digits = (int)(value % BILLION);
        value /= BILLION;

        // turn the 9 digits into a quad (so we can do the right thing later)
        decQuadFromInt32(&nine_quad_digits, nine_digits);

        // set our multiplier to be the right size, starting with 1e0
        decQuadSetExponent(&multiplier, set, multiplier_exponent);

        // decDoubleFMA(r, x, y, z, set)
        // Calculates the fused multiply-add x * y + z and places the result in r
        decQuadFMA(&result, &multiplier, &nine_quad_digits, &result, set);

        // so we have the right multiplier for next time
        multiplier_exponent += 9;
    }

    // now put the sign and exponent in place, and we should be done
    if (is_negative) {
        decQuadCopyNegate(&result, &result);
    }
    decQuadSetExponent(&result, set, exp);

    ION_DECIMAL_TEST_AND_RESTORE_STATUS(saved_status, set, DEC_Inexact);

    decQuadCopy(p_quad, &result);

    iRETURN;
}

void ion_quad_get_packed_and_exponent_from_quad(const decQuad *quad_value, uint8_t *p_packed, int32_t *p_exp)
{
	decQuadToPacked(quad_value, p_exp, p_packed);
}

uint64_t decQuadToUInt64(const decQuad *df, decContext *set, BOOL *p_overflow, BOOL *p_is_negative)
{
  enum rounding saveround;             // saver
  uint32_t savestatus;                 // ..
  decQuad  zero, billion;              // constants we need as decimal
  decQuad  quad_result, remainder;     // work decimal values
  uint64_t  int64_result;              // ..
  uint64_t  nine_digits;
  int      ii, count;


  // this version is much more limited than the original
  assert(decQuadIsFinite(df));
  assert(decQuadIsInteger(df));

  *p_overflow = FALSE;

  decQuadZero(&zero);                 // make 0E+0

  decQuadCopy(&quad_result, df);

  if ((*p_is_negative = decQuadIsSigned(&quad_result))) {
    decQuadMinus(&quad_result, &quad_result, set);
  }

  decQuadFromInt32(&billion, BILLION);

  saveround  = set->round;            // save rounding mode ..
  savestatus = set->status;           // .. and status
  set->round = DEC_ROUND_FLOOR;       // we want to truncate, until further notice

  int64_result = 0;

  for (count = 0; 1; count++) {
    if (decQuadIsZero(&quad_result)) break;

    // decDoubleRemainder(r, x, y, set) - Integer-divides x by y and
    // places the remainder from the division in r.
    decQuadRemainder(&remainder, &quad_result, &billion, set);
    decQuadDivide(&quad_result, &quad_result, &billion, set);

    set->status = 0;                    // clear

    // [this may fail] - otherwise it's the int value, rounded
    decQuadQuantize(&quad_result, &quad_result, &zero, set);

    // this test might not really be worth it, but it seems likely
    // since code like toInt32 is very expensive and is zero is comparatively
    // cheap - but it may not be especially likely - hmmm
    if (!decQuadIsZero(&remainder)) {
      nine_digits = (uint64_t)decQuadToInt32(&remainder, set, set->round);
      if (count >= 2) {
        if (count > 2 // At least 27 decimal digits have been retrieved, so this overflows 64 bits.
            || nine_digits > UINT64_TOP_2_DIGITS // At least 18 decimal digits; compare the most significant.
            || (nine_digits == UINT64_TOP_2_DIGITS && int64_result > UINT64_BOTTOM_18_DIGITS) // Up to 20 digits; compare top two and bottom 18.
        ) {
          *p_overflow = TRUE;
          decQuadZero(&quad_result);
          break;
        }
      }
      for (ii=0; ii<count; ii++) {
        nine_digits *= BILLION;
      }
      int64_result += nine_digits;
    }
  }

  set->round = saveround;             // restore rounding mode ..
  set->status = savestatus;           // .. and status

  return int64_result;

} // decQuadToUInt64

double decQuadToDouble(const decQuad *dec, decContext *set)
{
  double  double_value;
  int64_t value;
  int     exp_base_10, exp_base_2 = 0;
  int     top3_nibbles, shift_nibble, shift;
  BOOL is_negative = 0;
  decQuad mantissa;
  BOOL overflow;

  ion_quad_get_exponent_and_shift(dec, set, &mantissa, &exp_base_10);
  value = decQuadToUInt64(&mantissa, set, &overflow, &is_negative);

  // shift "value" until we have a sufficiently small number of bits to 
  // fit into the IEEE754 binary float - 64 bit == 53 bit (52 + 1)
  // so the top 11 bits should be 0 (note that since we're positive
  // at least the top bit will be positive at this point
  top3_nibbles = (int)(value >> (64-12));  // does assume int is at least 16 bits
  assert((top3_nibbles & 0xF000) == 0);
  if (top3_nibbles & 0x0FFE) {
    if (top3_nibbles & 0x0F00) {
      shift_nibble = (top3_nibbles & 0x0F00) >> 8;
      // the dec_quad_helper_shift_table_for_nibbles table is how many
      // bits we have to shift off to shift the most significant bit
      // below the nibble - this table could be bigger (and pick up
      // all 10 bits, but I don't feel like filling out a table with 
      // 1024 entries at the moment - this isn't really a high use
      // function or likely to be an time critical one either)
      shift = dec_quad_helper_shift_table_for_nibbles[shift_nibble];
      exp_base_2 += shift;
      value >>= shift;
      assert((value & 0xF000000000000000) == 0); // the top nibble should be clear now
    }
    if (top3_nibbles & 0x00F0) {
      shift_nibble = (top3_nibbles & 0x00F0) >> 4;
      shift = dec_quad_helper_shift_table_for_nibbles[shift_nibble];
      exp_base_2 += shift;
      value >>= shift;
      assert((value & 0xFF00000000000000) == 0); // the top 2 nibbles should be clear now
    }
    if (top3_nibbles & 0x000E) {
        // we remove an extra bit because we only care about the top 3,
        // the top 3 act like the top 4 with the top bit 0, which what we have now
      shift_nibble = (top3_nibbles & 0x000E) >> 5;
      shift = dec_quad_helper_shift_table_for_nibbles[shift_nibble];
      exp_base_2 += shift;
      value >>= shift;
    }
  }
  assert((value & 0xFFE0000000000000) == 0); // the top 11 bits should be clear now

  double_value = (double)value; // this should fit now

  // now we adjust the value to account for any bits we shifted off
  if (exp_base_2) {
    double_value *= pow(2, exp_base_2);
  }

  // and apply the decimal (base 10) exponent to get the abs value
  if (exp_base_10) {
    double_value *= pow(10, exp_base_10);
  }

  // and finally apply the sign, now we should have the acual value, in so
  // far as a decimal can account for it - modulo some edges Ion shouldn't encounter
  if (is_negative) {
      double_value = -double_value;
  }

  return double_value;
}
