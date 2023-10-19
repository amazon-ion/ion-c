#pragma once

#include "../common.h"

#include "cbor.h"
#include "cbor/encoding.h"
#include "cbor/ints.h"

namespace libcbor {
   typedef ::Tape<cbor_type> Tape;
   typedef Tape::iterator TapeIter;
   typedef ::TapeData<cbor_type> TapeData;

   class SerContext {
      uint8_t *buffer_start;
      uint8_t *buffer_end;
      uint8_t *buffer_current;

      public:
      SerContext(uint8_t *buffer, size_t len) {
         buffer_current = buffer_start = buffer;
         buffer_end = buffer + len;
      }

      const uint8_t *begin() const { return buffer_start; }
      const uint8_t *end() const { return buffer_end; }
      unsigned char *current() const { return (unsigned char *)buffer_current; }
      const size_t remaining_size() const { return buffer_end - buffer_current; }
      void advance(size_t s) { buffer_current += s; }
      size_t bytes_written() const { return buffer_current - buffer_start; }
   };

   class LibCBOR : public Library {
      Tape _tape;
      size_t _data_size;
      public:
         const char *name() const override { return "libcbor"; }

         static constexpr char const *suffix() {
            return "cbor";
         }

         size_t data_size() const override {
            return _data_size;
         }

         ValueStats deserialize(uint8_t *input, size_t in_size) override {
            ValueStats stats = {0};

            struct cbor_load_result result;
            cbor_item_t *item = cbor_load(input, in_size, &result);

            if (result.error.code != CBOR_ERR_NONE) {
               return stats; // Not handling errors, zero stats should highlight a problem.
            }

            stats = process_value(item);

            cbor_decref(&item);

            return stats;
         }

         ValueStats process_value(cbor_item_t *val) {
            ValueStats stats = {0};

            if (val == nullptr) {
               return stats;
            }

            switch (cbor_typeof(val)) {
               case CBOR_TYPE_UINT:
                  stats.num_nums++;
                  break;
               case CBOR_TYPE_NEGINT:
                  stats.num_nums++;
                  break;
               case CBOR_TYPE_BYTESTRING:
                  break;
               case CBOR_TYPE_STRING: {
                  std::string str((const char *)cbor_string_handle(val), (size_t)cbor_string_length(val));
                  stats.str_size += str.length();
                  stats.num_strs++;
                  break;
               }
               case CBOR_TYPE_ARRAY: {
                  auto vstats = process_list(val);
                  stats += vstats;
                  stats.num_lists++;
                  break;
               }
               case CBOR_TYPE_MAP: {
                  auto vstats = process_object(val);
                  stats += vstats;
                  stats.num_objs++;
                  break;
               }
               case CBOR_TYPE_TAG:
                  break;
               case CBOR_TYPE_FLOAT_CTRL: {
                  if (!cbor_float_ctrl_is_ctrl(val)) {
                     stats.num_nums++;
                  } else {
                    switch (cbor_ctrl_value(val)) {
                       case CBOR_CTRL_TRUE:
                       case CBOR_CTRL_FALSE:
                          stats.num_bools++;
                          break;
                       case CBOR_CTRL_NULL:
                          stats.num_nulls++;
                          break;
                    }
                  }
                  break;
               }
            }

            return stats;
         }

         ValueStats process_list(cbor_item_t *list) {
            ValueStats stats = {0};

            size_t len = cbor_array_size(list);
            cbor_item_t **items = cbor_array_handle(list);
            for (int i=0; i < len; ++i) {
               auto valstats = process_value(items[i]);
               stats += valstats;
            }

            return stats;
         }

         ValueStats process_object(cbor_item_t *obj) {
            ValueStats stats = {0};

            size_t len = cbor_map_size(obj);
            struct cbor_pair *pairs = cbor_map_handle(obj);
            for (int i=0; i < len; ++i) {
               auto keystat = process_value(pairs[i].key);
               auto valstats = process_value(pairs[i].value);

               stats += keystat;
               stats += valstats;
            }
            
            return stats;
         }

         void load_data(uint8_t *input, size_t in_size) override {
            struct cbor_load_result result;
            cbor_item_t *item = cbor_load(input, in_size, &result);
            _tape.clear();

            if (result.error.code != CBOR_ERR_NONE) {
               printf("ERROR loading cbor data: %d\n", result.error.code);
               exit(1);
            }
            load_value(item, _tape, std::nullopt);
            _data_size = in_size;
            cbor_decref(&item);
         }

         void load_value(cbor_item_t *item, Tape &tape, std::optional<std::string> name) {
            if (item == nullptr) {
               return;
            }

            switch (cbor_typeof(item)) {
               case CBOR_TYPE_MAP: {
                  size_t num_fields = cbor_map_size(item);
                  tape.push_back(TapeData(CBOR_TYPE_MAP).with_field_name(name).with_container_size(num_fields));
                  load_map(item, tape);
                  tape.push_back(TapeData(CBOR_TYPE_MAP).with_field_name(name).with_container_size(num_fields).that_closes());
                  break;
               }
               case CBOR_TYPE_ARRAY: {
                  size_t num_items = cbor_array_size(item);
                  tape.push_back(TapeData(CBOR_TYPE_ARRAY).with_field_name(name).with_container_size(num_items));
                  load_array(item, tape);
                  tape.push_back(TapeData(CBOR_TYPE_ARRAY).with_field_name(name).with_container_size(num_items).that_closes());
                  break;
               }
               case CBOR_TYPE_STRING: {
                  std::string str((const char *)cbor_string_handle(item), (size_t)cbor_string_length(item));
                  tape.push_back(TapeData(CBOR_TYPE_STRING).with_field_name(name).with_value(std::make_optional(std::make_any<std::string>(str))));
                  break;
               }
               case CBOR_TYPE_UINT: {
                  std::any i;
                  switch (cbor_int_get_width(item)) {
                     case CBOR_INT_8:
                        i = std::make_any<uint8_t>(cbor_get_uint8(item));
                        break;
                     case CBOR_INT_16:
                        i = std::make_any<uint16_t>(cbor_get_uint16(item));
                        break;
                     case CBOR_INT_32:
                        i = std::make_any<uint32_t>(cbor_get_uint32(item));
                        break;
                     case CBOR_INT_64:
                        i = std::make_any<uint64_t>(cbor_get_uint64(item));
                        break;
                  }
                  tape.push_back(TapeData(CBOR_TYPE_UINT).with_field_name(name).with_value(std::make_optional(i)));
                  break;
               }
               case CBOR_TYPE_NEGINT: {
                  std::any i;
                  switch (cbor_int_get_width(item)) {
                     case CBOR_INT_8:
                        i = std::make_any<uint8_t>(cbor_get_uint8(item));
                        break;
                     case CBOR_INT_16:
                        i = std::make_any<uint16_t>(cbor_get_uint16(item));
                        break;
                     case CBOR_INT_32:
                        i = std::make_any<uint32_t>(cbor_get_uint32(item));
                        break;
                     case CBOR_INT_64:
                        i = std::make_any<uint64_t>(cbor_get_uint64(item));
                        break;
                  }
                  tape.push_back(TapeData(CBOR_TYPE_NEGINT).with_field_name(name).with_value(std::make_optional(i)));
                  break;
               }
               case CBOR_TYPE_FLOAT_CTRL: {
                  std::optional<std::any> val = std::nullopt;
                  if (!cbor_float_ctrl_is_ctrl(item)) {
                     std::any f;
                     switch (cbor_float_get_width(item)) {
                        case CBOR_FLOAT_16:
                           val = std::make_optional(std::make_any<float>(cbor_float_get_float2(item)));
                           break;
                        case CBOR_FLOAT_32:
                           val = std::make_optional(std::make_any<float>(cbor_float_get_float4(item)));
                           break;
                        case CBOR_FLOAT_64:
                           val = std::make_optional(std::make_any<double>(cbor_float_get_float8(item)));
                           break;
                        default: exit(1); break; // Unreachable.
                     }
                  } else if (cbor_is_bool(item)) {
                     val = std::make_optional(std::make_any<bool>(cbor_get_bool(item)));
                  } else if (cbor_is_null(item)) {
                     val = std::make_optional(std::make_any<void*>((void*)NULL));
                  } else if (cbor_is_undef(item)) {
                     val = std::nullopt;
                  }
                  tape.push_back(TapeData(CBOR_TYPE_FLOAT_CTRL).with_field_name(name).with_value(val));
                  break;
               }
               default:
                  printf("ERROR: Unimplemented type: %d\n", cbor_typeof(item));
                  exit(1);
            }
         }

         void load_map(cbor_item_t *item, Tape &tape) {
            size_t len = cbor_map_size(item);
            struct cbor_pair *pairs = cbor_map_handle(item);
            for (int i=0; i < len; ++i) {
               if (cbor_typeof(pairs[i].key) != CBOR_TYPE_STRING) {
                  printf("ERROR: Non-string key, not supported\n");
                  exit(1);
               }
               std::string field_name((const char *)cbor_string_handle(pairs[i].key), (size_t)cbor_string_length(pairs[i].key));
               load_value(pairs[i].value, tape, std::make_optional(field_name));
            }
         }

         void load_array(cbor_item_t *item, Tape &tape) {
            size_t len = cbor_array_size(item);
            auto array = cbor_array_handle(item);
            for (int i=0; i < len; ++i) {
               load_value(array[i], tape, std::nullopt);
            }
         }

         ValueStats serialize_loaded_data(uint8_t *output, size_t &len, bool pretty) override {
            ValueStats stats = {0};
            SerContext ctx(output, len);
            auto node_itr = _tape.begin();

            // Build the hierarchy of our data.
            stats = serialize_value(ctx, node_itr, std::nullopt);

            len = ctx.bytes_written();

            stats.serde_bytes += len;

            return stats;
         }

         ValueStats serialize_value(SerContext &ctx, TapeIter &node_itr, std::optional<std::string> name) {
            ValueStats stats = {0};

            if (node_itr->field_name.has_value()) {
               size_t len = node_itr->field_name->length();
               size_t s = cbor_encode_string_start(len, ctx.current(), ctx.remaining_size());
               ctx.advance(s);
               memcpy(ctx.current(), node_itr->field_name->c_str(), len);
               ctx.advance(len);
            }

            switch (node_itr->tpe) {
               case CBOR_TYPE_ARRAY: 
                  stats = serialize_array(ctx, node_itr);
                  break;
               case CBOR_TYPE_MAP: {
                  stats = serialize_object(ctx, node_itr);
                  break;
               }
               case CBOR_TYPE_STRING: {
                  std::string str = std::any_cast<std::string>(node_itr->value.value());
                  size_t s = cbor_encode_string_start(str.length(), ctx.current(), ctx.remaining_size());
                  ctx.advance(s);
                  memcpy(ctx.current(), str.c_str(), str.length());
                  ctx.advance(str.length());
                  break;
               }
               case CBOR_TYPE_UINT: {
                  size_t s = 0;
                  auto i = node_itr->value.value();
                  if (i.type() == typeid(uint8_t))
                     s = cbor_encode_uint8(std::any_cast<uint8_t>(i), ctx.current(), ctx.remaining_size());
                  else if (i.type() == typeid(uint16_t))
                     s = cbor_encode_uint16(std::any_cast<uint16_t>(i), ctx.current(), ctx.remaining_size());
                  else if (i.type() == typeid(uint32_t))
                     s = cbor_encode_uint32(std::any_cast<uint32_t>(i), ctx.current(), ctx.remaining_size());
                  else if (i.type() == typeid(uint64_t))
                     s = cbor_encode_uint64(std::any_cast<uint64_t>(i), ctx.current(), ctx.remaining_size());
                  else
                     printf("UNKNOWN UINT\n");
                  ctx.advance(s);
                  break;
               }
               case CBOR_TYPE_NEGINT: {
                  size_t s = 0;
                  auto i = node_itr->value.value();
                  if (i.type() == typeid(uint8_t))
                     s = cbor_encode_negint8(std::any_cast<uint8_t>(i), ctx.current(), ctx.remaining_size());
                  else if (i.type() == typeid(uint16_t))
                     s = cbor_encode_negint16(std::any_cast<uint16_t>(i), ctx.current(), ctx.remaining_size());
                  else if (i.type() == typeid(uint32_t))
                     s = cbor_encode_negint32(std::any_cast<uint32_t>(i), ctx.current(), ctx.remaining_size());
                  else if (i.type() == typeid(uint64_t))
                     s = cbor_encode_negint64(std::any_cast<uint64_t>(i), ctx.current(), ctx.remaining_size());
                  else
                     printf("UNKNOWN NEGINT\n");
                  ctx.advance(s);
                  break;
               }
               case CBOR_TYPE_FLOAT_CTRL: {
                  size_t s = 0;
                  if (node_itr->value.has_value()) {
                     auto val = node_itr->value.value();
                     if (val.type() == typeid(float))
                        s = cbor_encode_single(std::any_cast<float>(val), ctx.current(), ctx.remaining_size());
                     else if (val.type() == typeid(double))
                        s = cbor_encode_double(std::any_cast<double>(val), ctx.current(), ctx.remaining_size());
                     else if (val.type() == typeid(bool))
                        s = cbor_encode_bool(std::any_cast<bool>(val), ctx.current(), ctx.remaining_size());
                     else if (val.type() == typeid(void*))
                        s = cbor_encode_null(ctx.current(), ctx.remaining_size());
                     else {
                        printf("UNKNOWN FLOAT CTRL\n");
                        exit(1);
                     }
                  } else {
                     s = cbor_encode_undef(ctx.current(), ctx.remaining_size());
                  }
                  ctx.advance(s);
                  break;
               }
               case CBOR_TYPE_BYTESTRING:
                  printf("BYTESTRING NOT ENCODED\n");
                  break;
            }

            return stats;
         }

         ValueStats serialize_array(SerContext &ctx, TapeIter &node_itr) {
            ValueStats stats = {0};
            size_t num_items = node_itr->container_size;
            size_t s = cbor_encode_array_start(num_items, ctx.current(), ctx.remaining_size());
            ctx.advance(s);
            ++node_itr;

            for (int i = 0; i < num_items; ++i) {
               serialize_value(ctx, node_itr, std::nullopt);
               ++node_itr;
            }

            return stats;
         }

         ValueStats serialize_object(SerContext &ctx, TapeIter &node_itr) {
            ValueStats stats = {0};
            size_t num_fields = node_itr->container_size;
            size_t s = cbor_encode_map_start(num_fields, (unsigned char *)ctx.current(), ctx.remaining_size());
            ctx.advance(s);
            ++node_itr;

            for (int i = 0; i < num_fields; ++i) {
               std::string field_name;
               serialize_value(ctx, node_itr, field_name);
               ++node_itr;
            }
            
            return stats;
         }
   };
}
