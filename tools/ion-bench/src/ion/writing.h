#pragma once

#include <string>
#include <optional>
#include <any>

#include "common.h"
#include "ionc.h"

#include "ionc/ion.h"
#include "ionc/ion_errors.h"
#include "ionc/ion_writer.h"

namespace ion {

template <Format format>
class BufferWriter {
   private:
      hWRITER _writer;
      ION_STREAM *_stream;
   public:
      /// Constructor..
      BufferWriter(uint8_t *buffer, size_t bufsize, bool pretty) {
         ION_WRITER_OPTIONS options = {0};
         ION_STREAM *stream = nullptr;
         hWRITER writer = nullptr;

         options.output_as_binary = (format == Format::Binary);
         options.pretty_print = pretty;
         options.indent_size = 2;
         options.indent_with_tabs = FALSE;
         options.small_containers_in_line = TRUE;

         auto err = ion_stream_open_buffer(buffer, bufsize, 0, FALSE, &stream);
         if (err == IERR_OK) {
            err = ion_writer_open(&writer, stream, &options);
            if (err == IERR_OK) {
               _stream = stream;
               _writer = writer;
            } else {
               ion_stream_close(stream);
            }
         }
      }

      // ..
      ~BufferWriter() {
         if (_writer != nullptr) {
            ion_writer_close(_writer);
            ion_stream_close(_stream);
         }
      }

      size_t bytes_written() const {
         return ion_stream_get_position(_stream);
      }

      size_t flush() {
         SIZE bytes = 0;
         ion_writer_flush(_writer, &bytes);
         return bytes;
      }

      size_t depth() const {
         SIZE s = 0;
         iERR err = ion_writer_get_depth(_writer, &s);
         if (err != IERR_OK) {
            printf("ERROR getting depth: %d", err);
         }
         return (size_t)s;
      }

      iERR start_container(ION_TYPE tpe) {
         iERR err = ion_writer_start_container(_writer, tpe);
         if (err != IERR_OK)
            printf("ERROR starting container of type: %p, err: %d\n", tpe, err);
         return err;
      }

      iERR finish_container() {
         iERR err = ion_writer_finish_container(_writer);
         if (err != IERR_OK)
            printf("ERROR finishing container: %d\n", err);
         return err;
      }
      
      iERR set_field_name(std::optional<std::string> &field) {
         if (field.has_value()) {
            ION_STRING str;
            ion_string_assign_cstr(&str, (char *)field->c_str(), (SIZE)field->length());
            return ion_writer_write_field_name(_writer, &str);
         } else {
            return ion_writer_clear_field_name(_writer);
         }
      }

      iERR close() {
         iERR err = ion_writer_close(_writer);
         if (err != IERR_OK)
            printf("Error closing writer: %d\n", err);
         err = ion_stream_close(_stream);
         if (err != IERR_OK)
            printf("Error closing stream: %d\n", err);
         _writer = nullptr;
         _stream = nullptr;
         return err;
      }

      iERR write_annotations(const std::vector<std::string> &anns) {
         iERR err = IERR_OK;
         if (anns.size() > 0) {
            ION_STRING *strs = new ION_STRING[anns.size()];
            for (int i=0; i < anns.size(); i++) {
               ION_STRING ann = { .length = (SIZE)anns[i].length(), .value = (BYTE*)anns[i].c_str() };
               ion_string_copy_to_owner(_writer, &strs[i], &ann);
            }
            err = ion_writer_write_annotations(_writer, strs, (SIZE)anns.size());
            delete[] strs;
         }
         return err;
      }

      iERR write_null_container(ION_TYPE tpe) {
         iERR err = ion_writer_write_typed_null(_writer, tpe);
         if (err != IERR_OK)
            printf("ERROR writing null container: %d\n", err);
         return err;
      }

      iERR write(const std::optional<std::string> &s) {
         ION_STRING ion_str;
         if (s) {
            ion_string_assign_cstr(&ion_str, (char *)s->c_str(), s->length());
            return ion_writer_write_string(_writer, &ion_str);
         } else {
            return ion_writer_write_typed_null(_writer, tid_STRING);
         }
      }

      iERR write(const std::optional<int> &i) {
         if (i) {
            return ion_writer_write_int64(_writer, *i);
         } else {
            return ion_writer_write_typed_null(_writer, tid_INT);
         }
      }

      iERR write(const std::optional<int64_t> &i) {
         if (i) {
            return ion_writer_write_int64(_writer, *i);
         } else {
            return ion_writer_write_typed_null(_writer, tid_INT);
         }
      }

      iERR write(const std::optional<float> &f) {
         if (f) {
            return ion_writer_write_float(_writer, *f);
         } else {
            return ion_writer_write_typed_null(_writer, tid_FLOAT);
         }
      }

      iERR write(const std::optional<bool> &b) {
         if (b) {
            return ion_writer_write_bool(_writer, *b);
         } else {
            return ion_writer_write_typed_null(_writer, tid_BOOL);
         }
      }

      iERR write(const std::optional<double> &d) {
         if (d) {
            return ion_writer_write_double(_writer, *d);
         } else {
            return ion_writer_write_typed_null(_writer, tid_FLOAT);
         }
      }

      iERR write(const std::optional<IonDecimalPtr> &d) {
         if (d.has_value()) {
            return ion_writer_write_ion_decimal(_writer, (*d).get());
         } else {
            return ion_writer_write_typed_null(_writer, tid_DECIMAL);
         }
      }

      iERR write(const std::optional<Symbol> &sym) {
         if (sym.has_value()) {
            ION_STRING str = {0};
            ion_string_assign_cstr(&str, (char *)sym->value.c_str(), sym->value.length());
            return ion_writer_write_symbol(_writer, &str);
         } else {
            return ion_writer_write_typed_null(_writer, tid_SYMBOL);
         }
      }

      iERR write(const std::optional<std::shared_ptr<ION_TIMESTAMP>> &ts) {
         if (ts.has_value()) {
            return ion_writer_write_timestamp(_writer, ts.value().get());
         } else {
            return ion_writer_write_typed_null(_writer, tid_TIMESTAMP);
         }
      }
};

#define WRITE_ION_TYPE(writer, tid, tpe, val)                                                      \
   case tid: {                                                                                     \
      std::optional<tpe> t = val.value ?                                                           \
                std::make_optional(std::any_cast<tpe>(val.value.value()))                          \
              : std::nullopt;                                                                      \
      err = writer.write(t);                                                                       \
      if (err != IERR_OK) { printf("ERROR writing " #tpe ": %d\n", err); }                         \
      break;                                                                                       \
   }

template <Format F>
iERR write(BufferWriter<F> &writer, const IonData &val) {
   iERR err = IERR_NOT_IMPL;
   writer.write_annotations(val.annotations);
   switch (val.tpe) {
      WRITE_ION_TYPE(writer, tid_STRING_INT, std::string, val);
      WRITE_ION_TYPE(writer, tid_INT_INT, int64_t, val);
      WRITE_ION_TYPE(writer, tid_FLOAT_INT, double, val);
      WRITE_ION_TYPE(writer, tid_DECIMAL_INT, IonDecimalPtr, val);
      WRITE_ION_TYPE(writer, tid_BOOL_INT, bool, val);
      WRITE_ION_TYPE(writer, tid_SYMBOL_INT, Symbol, val);
      WRITE_ION_TYPE(writer, tid_TIMESTAMP_INT, std::shared_ptr<ION_TIMESTAMP>, val);
      default:
         printf("Attempt to write unknown type: %ld\n", val.tpe);
         break;
   }
   return err;
}
#undef WRITE_ION_TYPE

}
