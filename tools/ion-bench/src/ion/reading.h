#pragma once

#include "ionc.h"

const int MAX_DECIMAL_DIGITS = 10000; // Should be enough for anybody.. right?..

namespace ion {
   struct IonNull {
      ION_TYPE tpe;
   };

   template <Format format>
   class BufferReader {
      private:
         hREADER _reader;
         ION_TYPE _current;
         decContext _context;
      public:
         BufferReader(uint8_t *buffer, size_t bufsize) {
            ION_READER_OPTIONS options = {0};
            this->_context = {
               MAX_DECIMAL_DIGITS,   /* max digits */
               DEC_MAX_MATH,         /* max exponent */
               -DEC_MAX_MATH,        /* min exponent */
               DEC_ROUND_HALF_EVEN,  /* rounding mode */
               DEC_Errors,           /* trap conditions */
               0,                    /* status flags */
               0                     /* apply exponent clamp? */
            };
            options.decimal_context = &(this->_context);
            hREADER reader = nullptr;

            auto err = ion_reader_open_buffer(&reader, buffer, bufsize, &options);
            if (err == IERR_OK) {
               _reader = reader;
            } else {
               printf("ERROR\n");
            }
         }

         ~BufferReader() {
            ion_reader_close(_reader);
         }

         ION_TYPE current_type() const {
            return _current;
         }

         std::optional<std::string> field_name() const {
            ION_STRING ionstr = {0};
            iERR err = ion_reader_get_field_name(_reader, &ionstr);
            if (err == IERR_OK) {
               std::string value((char *)ionstr.value, ionstr.length);
               return std::make_optional(value);
            }
            return std::nullopt;
         }

         iERR next() {
            auto err = ion_reader_next(_reader, &_current);
            return err;
         }

         bool in_struct() {
            BOOL in = FALSE;
            auto err = ion_reader_is_in_struct(_reader, &in);
            if (err != IERR_OK)
               printf("ERROR calling is_in_struct: %d\n", err);
            return in == TRUE;
         }

         iERR step_in() {
            return ion_reader_step_in(_reader);
         }

         iERR step_out() {
            return ion_reader_step_out(_reader);
         }

         int depth() const {
            int depth = 0;
            ion_reader_get_depth(_reader, &depth);
            return depth;
         }

         bool is_null() const {
            BOOL is_null = FALSE;
            auto err = ion_reader_is_null(_reader, &is_null);
            if (err != IERR_OK)
               printf("ERROR is_null: %d", err);
            return (is_null == TRUE);
         }

         std::vector<std::string> get_annotations() {
            std::vector<std::string> annot;
            BOOL has_anns = FALSE;
            SIZE count = 0;

            ion_reader_has_any_annotations(_reader, &has_anns);
            if (has_anns == TRUE) {
               ion_reader_get_annotation_count(_reader, &count);
               ION_STRING *syms = new ION_STRING[count];
               ion_reader_get_annotations(_reader, syms, count, &count);
               for (int i=0; i < count; i++) {
                  annot.push_back(std::string((char *)syms[i].value, syms[i].length));
               }
               delete[] syms;
            }
            return annot;
         }

         iERR read(std::string &val) {
            ION_STRING ionstr = {0};
            auto err = ion_reader_read_string(_reader, &ionstr);
            val.assign((char *)ionstr.value, ionstr.length);
            return err;
         }

         iERR read(int32_t &val) {
            return ion_reader_read_int(_reader, &val);
         }

         iERR read(int64_t &val) {
            return ion_reader_read_int64(_reader, &val);
         }

         iERR read(IonNull &val) {
            ION_TYPE tpe;
            auto err = ion_reader_read_null(_reader, &tpe);
            val.tpe = tpe;
            return err;
         }

         iERR read(std::shared_ptr<ION_DECIMAL> &val) {
            ION_DECIMAL original;
            auto err = ion_reader_read_ion_decimal(_reader, &original);
            if (err != IERR_OK)
               return err;

            ION_DECIMAL *dec = (ION_DECIMAL *)malloc(sizeof(ION_DECIMAL));
            err = ion_decimal_copy(dec, &original);
            if (err != IERR_OK) {
               free(dec);
               return err;
            }
            val.reset(dec, IonDecimalDeleter());
            return err;
         }

         iERR read(double &val) {
            return ion_reader_read_double(_reader, &val);
         }

         iERR read(bool &val) {
            BOOL b = FALSE;
            auto err = ion_reader_read_bool(_reader, &b);
            if (err != IERR_OK)
               return err;
            val = (b == TRUE);
            return IERR_OK;
         }

         iERR read(Symbol &val) {
            ION_SYMBOL sym;
            auto err = ion_reader_read_ion_symbol(_reader, &sym);
            if (err != IERR_OK) {
               return err;
            }

            ION_STRING ionstr = {0};
            err = ion_reader_read_string(_reader, &ionstr);
            if (err != IERR_OK)
               return err;

            std::string value((char *)ionstr.value, ionstr.length);
            val.value.assign((char *)ionstr.value, ionstr.length);

            return IERR_OK;
         }

         iERR read(std::shared_ptr<ION_TIMESTAMP> &val) {
            ION_TIMESTAMP *ts = (ION_TIMESTAMP*)malloc(sizeof(ION_TIMESTAMP));
            auto err = ion_reader_read_timestamp(_reader, ts);
            if (err != IERR_OK)
               return err;
            val.reset(ts, free);
            return IERR_OK;
         }
   };

   template <typename T, Format F>
   T read(BufferReader<F> &reader) {
      T val;
      auto err = reader.read(val);
      if (err != IERR_OK)
         printf("ERROR reading value: %d\n", err);
      return val;
   }

}
