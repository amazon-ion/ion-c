#pragma once

#include "ionc/ion.h"

#include "common.h"
#include "ionc/ion_types.h"
#include "writing.h"
#include "reading.h"

#include <vector>
#include <string>
#include <any>
#include <optional>

namespace ion {

template <Format format>
class IonC : public Library {
   private:
      std::vector<IonData> _tape;
      size_t loaded_data_size;
   public:
      virtual const char *name() const override {
         switch (format) {
            case Format::Binary:
               return "ion-c-binary";
            case Format::Text:
               return "ion-c-text";
            case Format::JSON:
               return "ion-c-json";
         }
      }

      static constexpr char const *suffix() {
         switch (format) {
            case Format::Binary:
               return "10n";
            case Format::Text:
               return "ion";
            case Format::JSON:
               return "json";
         }
      }

      size_t data_size() const override {
         return this->loaded_data_size;
      }

   // Deserializes the Ion data that is contained in the `input`
   // buffer. The intent is to visit every node in the data,
   // properly decoding it, but ultimately throwing the values
   // away.
   ValueStats deserialize(uint8_t *input, size_t in_size) override {
      ValueStats stats = {0};
      BufferReader<format> reader(input, in_size);
      auto err = reader.next();

      while (reader.current_type() != tid_EOF || reader.depth() > 0) {
         if (reader.depth() > 0 && reader.current_type() != tid_EOF) {
            std::optional<std::string> name = reader.field_name();
            // printf(" %s = ", name.c_str());
         }
         switch (ION_TYPE_INT(reader.current_type())) {
            case tid_STRING_INT: {
               if (!reader.is_null()) {
                  std::string value = read<std::string>(reader);
                  stats.str_size += value.length();
               }
               stats.num_strs++;
               break;
            }
            case tid_INT_INT: {
               if (!reader.is_null()) {
                  int64_t value = read<int64_t>(reader);
               }
               stats.num_nums++;
               break;
            }
            case tid_FLOAT_INT: {
               if (!reader.is_null())
                  double d = read<double>(reader);
               stats.num_nums++;
               break;
            }
            case tid_DECIMAL_INT: {
               if (!reader.is_null()) {
                  auto dec = read<std::shared_ptr<ION_DECIMAL>>(reader);
               }
               stats.num_nums++;
               break;
            }
            case tid_BOOL_INT: {
               stats.num_bools++;
               break;
            }
            case tid_NULL_INT: {
               if (!reader.is_null())
                  auto null = read<IonNull>(reader);
               stats.num_nulls++;
               break;
            }
            case tid_STRUCT_INT:
               stats.num_objs++;
               // Fall through to step-in
            case tid_LIST_INT:
            case tid_SEXP_INT:
            case tid_BLOB_INT:
            case tid_CLOB_INT:
               if (!reader.is_null())
                  reader.step_in();
               break;
            case tid_SYMBOL_INT: {
               auto sym = read<Symbol>(reader);
               break;
            }
            case tid_TIMESTAMP_INT:
               break;
            case tid_EOF_INT: {
               auto depth = reader.depth();
               if (depth > 0)
                  reader.step_out();

               if (depth <= reader.depth()) {
                  printf("Stepping out did not lower depth\n");
                  exit(1);
               }
               break;
            }
            default:
               printf("NO IDEA WHAT THIS IS: %p\n", reader.current_type());
               break;
         }
         // printf(" Type: %p\n", reader.current_type());
         auto err = reader.next();
         if (err != IERR_OK)
            printf("ERR: %d\n", err);
      }

      return stats;
   }


#  define TYPE_HANDLER(itpe, value_type) \
      case itpe:                         \
         data = IonData {                \
            .tpe = ion_type,             \
            .null_container = false,     \
            .field_name = field_name,    \
            .value = (reader.is_null()) ? std::nullopt : std::make_optional<std::any>(std::make_any<value_type>(read<value_type>(reader))), \
            .annotations = annotations   \
         };                              \
         break

   void load_data(uint8_t *input, size_t in_size) override {
      std::any value;
      int ion_type;
      std::optional<std::string> field_name;

      BufferReader<format> reader(input, in_size);
      auto err = reader.next();
      _tape.clear();

      int depth = 0;
      while (reader.current_type() != tid_EOF || reader.depth() > 0) {
         if (reader.in_struct() && reader.current_type() != tid_EOF) {
            field_name = reader.field_name();
         } else {
            field_name = std::nullopt;
         }

         std::vector<std::string> annotations = reader.get_annotations();
         ion_type = ION_TYPE_INT(reader.current_type());
         IonData data;
         switch (ion_type) {
            TYPE_HANDLER(tid_STRING_INT, std::string);
            TYPE_HANDLER(tid_INT_INT, int64_t);
            TYPE_HANDLER(tid_DECIMAL_INT, IonDecimalPtr);
            TYPE_HANDLER(tid_FLOAT_INT, double);
            TYPE_HANDLER(tid_BOOL_INT, bool);
            TYPE_HANDLER(tid_SYMBOL_INT, Symbol);
            TYPE_HANDLER(tid_TIMESTAMP_INT, std::shared_ptr<ION_TIMESTAMP>);
            case tid_STRUCT_INT:
            case tid_LIST_INT:
            case tid_SEXP_INT: {
               data = IonData {
                  .tpe = ion_type,
                  .null_container = true,
                  .field_name = field_name,
                  .value = std::nullopt,
               };
               if (!reader.is_null()) {
                  data.null_container = false;
                  reader.step_in();
                  depth++;
               }
               break;
            }
            case tid_EOF_INT:
               data = IonData {
                  .tpe = ion_type,
                  .field_name = std::nullopt,
                  .value = std::nullopt,
               };
               reader.step_out();
               depth--;
               break;
         }
         // print_iondata(data);
         _tape.push_back(data);
         err = reader.next();
         if (err != IERR_OK)
            printf("ERR: %d\n", err);
      }

      this->loaded_data_size = in_size;
      // printf("Finished loading tape: %d items, depth: %d\n", _tape.size(), depth);
   }
#  undef TYPE_HANDLER

   void indent(int n) { while(n>0) { printf(" "); n--; } }

#  define TYPE_HANDLER(itpe, value_type) \
   break

   ValueStats serialize_loaded_data(uint8_t *buffer, size_t &len, bool pretty) override {
      ValueStats stats = {0};
      BufferWriter<format> writer(buffer, len, pretty);
      int depth = 0;

      for (auto &val : this->_tape) {
         // indent(depth * 3); print_iondata(val);
         if (val.field_name.has_value()) {
            auto err = writer.set_field_name(val.field_name);
            if (err != IERR_OK)
               printf("ERROR setting field name: %d\n", err);
         }
         switch (val.tpe) {
            case tid_SEXP_INT:
            case tid_LIST_INT:
            case tid_STRUCT_INT:
               if (val.null_container) {
                  writer.write_null_container((ION_TYPE)val.tpe);
               } else {
                  depth++;
                  writer.start_container((ION_TYPE)val.tpe);
               }
               break;
            case tid_EOF_INT:
               if (writer.depth() > 0) {
                  writer.finish_container();
                  depth--;
               }
               break;
            default:
               write(writer, val);
               break;
         }
      }
      writer.flush();
      stats.serde_bytes += writer.bytes_written();
      writer.close();
      return stats;
   }
#  undef TYPE_HANDLER
};

}
