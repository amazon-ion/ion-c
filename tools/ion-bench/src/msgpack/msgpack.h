#pragma once

#include "../common.h"

#include <msgpack.h>

namespace msgpack {
   typedef ::Tape<msgpack_object_type> Tape;
   typedef Tape::iterator TapeIter;
   typedef ::TapeData<msgpack_object_type> TapeData;

   class MsgPackC : public Library {
      Tape _tape;
      size_t _data_size;

      public:
         const char *name() const override { return "MsgPack-C"; }

         static constexpr char const *suffix() {
            return "msgpack";
         }

         size_t data_size() const override {
            return _data_size;
         }

         ValueStats deserialize(uint8_t *input, size_t in_size) override {
            ValueStats stats = {0};

            msgpack_unpacked result;
            msgpack_unpack_return ret;
            size_t offset = 0;

            msgpack_unpacked_init(&result);

            ret = msgpack_unpack_next(&result, (const char *)input, in_size, &offset);
            while (ret == MSGPACK_UNPACK_SUCCESS) {
               auto valstats = process_value(&result.data);
               stats += valstats;
               ret = msgpack_unpack_next(&result, (const char *)input, in_size, &offset);
            }

            msgpack_unpacked_destroy(&result);
            return stats;
         }

         ValueStats process_value(msgpack_object *val) {
            ValueStats stats = {0};

            switch (val->type) {
               case MSGPACK_OBJECT_NIL:
                  stats.num_nulls++;
                  break;
               case MSGPACK_OBJECT_ARRAY: {
                  auto liststats = process_list(val);
                  stats += liststats;
                  break;
               }
               case MSGPACK_OBJECT_MAP: {
                  auto objstats = process_map(val);
                  stats += objstats;
                  stats.num_objs++;
                  break;
               }
               case MSGPACK_OBJECT_BOOLEAN:
                  stats.num_bools++;
                  break;
               case MSGPACK_OBJECT_POSITIVE_INTEGER:
                  break;
               case MSGPACK_OBJECT_NEGATIVE_INTEGER:
                  break;
               case MSGPACK_OBJECT_FLOAT32: {
                  double f = val->via.f64;
                  stats.num_nums++;
                  break;
               }
               case MSGPACK_OBJECT_FLOAT64:
                  stats.num_nums++;
                  break;
               case MSGPACK_OBJECT_STR:
                  stats.num_strs++;
                  break;
               case MSGPACK_OBJECT_BIN:
               case MSGPACK_OBJECT_EXT:
                  break;
            }

            return stats;
         }

         ValueStats process_map(msgpack_object *obj) {
            ValueStats stats = {0};

            if (obj->via.map.size != 0) {
               msgpack_object_kv *p = obj->via.map.ptr;
               msgpack_object_kv *const pend = obj->via.map.ptr + obj->via.map.size;
               for (; p < pend; ++p) {
                  auto valstats = process_value(&p->val);
                  stats += valstats;
               }
            }

            return stats;
         }

         ValueStats process_list(msgpack_object *list) {
            ValueStats stats = {0};

            if (list->via.array.size != 0) {
               msgpack_object *p = list->via.array.ptr;
               msgpack_object *const pend = p + list->via.array.size;
               for (; p < pend; ++p) {
                  auto valstats = process_value(p);
                  stats += valstats;
               }
            }

            return stats;
         }

         void load_data(uint8_t *input, size_t in_size) override {
            msgpack_unpacked result;
            msgpack_unpack_return ret;
            size_t offset = 0;

            msgpack_unpacked_init(&result);

            ret = msgpack_unpack_next(&result, (const char *)input, in_size, &offset);
            while (ret == MSGPACK_UNPACK_SUCCESS) {
               load_value(&result.data, std::nullopt, _tape);
               ret = msgpack_unpack_next(&result, (const char *)input, in_size, &offset);
            }
            _data_size = in_size;
         }

         void load_value(msgpack_object *val, std::optional<std::string> name, Tape &tape) {
            switch (val->type) {
               case MSGPACK_OBJECT_ARRAY:
                  tape.push_back(TapeData(MSGPACK_OBJECT_ARRAY).with_field_name(name).with_container_size(val->via.array.size));
                  load_list(val, tape);
                  tape.push_back(TapeData(MSGPACK_OBJECT_ARRAY).with_field_name(name).with_container_size(val->via.array.size).that_closes());
                  break;
               case MSGPACK_OBJECT_MAP: {
                  tape.push_back(TapeData(MSGPACK_OBJECT_MAP).with_field_name(name).with_container_size(val->via.map.size));
                  load_map(val, tape);
                  tape.push_back(TapeData(MSGPACK_OBJECT_MAP).with_field_name(name).with_container_size(val->via.map.size).that_closes());
                  break;
               }
               case MSGPACK_OBJECT_STR: {
                  std::string str(val->via.str.ptr, val->via.str.size);
                  tape.push_back(TapeData(MSGPACK_OBJECT_STR)
                        .with_field_name(name)
                        .with_value(std::make_optional(std::make_any<std::string>(str)))
                  );
                  break;
               }
               case MSGPACK_OBJECT_POSITIVE_INTEGER:
                  tape.push_back(TapeData(MSGPACK_OBJECT_POSITIVE_INTEGER)
                        .with_field_name(name)
                        .with_value(std::make_optional(std::make_any<uint64_t>(val->via.u64)))
                  );
                  break;
               case MSGPACK_OBJECT_NEGATIVE_INTEGER:
                  tape.push_back(TapeData(MSGPACK_OBJECT_NEGATIVE_INTEGER)
                        .with_field_name(name)
                        .with_value(std::make_optional(std::make_any<int64_t>(val->via.i64)))
                  );
                  break;
               case MSGPACK_OBJECT_FLOAT32:
               case MSGPACK_OBJECT_FLOAT64:
                  tape.push_back(TapeData(MSGPACK_OBJECT_FLOAT)
                        .with_field_name(name)
                        .with_value(std::make_optional(std::make_any<double>(val->via.f64)))
                  );
                  break;
               case MSGPACK_OBJECT_BOOLEAN: {
                  bool b = val->via.boolean;
                  tape.push_back(TapeData(MSGPACK_OBJECT_BOOLEAN)
                        .with_field_name(name)
                        .with_value(std::make_optional(std::make_any<bool>(b)))
                  );
                  break;
               }
               case MSGPACK_OBJECT_NIL:
                  tape.push_back(TapeData(MSGPACK_OBJECT_NIL)
                        .with_field_name(name)
                  );
                  break;
               case MSGPACK_OBJECT_EXT:
                  printf("ERROR: Ext not currently supported.\n");
                  break;
               case MSGPACK_OBJECT_BIN:
                  printf("ERROR: Bin not currently supported.\n");
                  break;
            }
         }

         void load_list(msgpack_object *list, Tape &tape) {
            if (list->via.array.size != 0) {
               msgpack_object *p = list->via.array.ptr;
               msgpack_object *const pend = p + list->via.array.size;
               for (; p < pend; ++p) {
                  load_value(p, std::nullopt, tape);
               }
            }
         }

         void load_map(msgpack_object *obj, Tape &tape) {
            if (obj->via.map.size != 0) {
               msgpack_object_kv *p = obj->via.map.ptr;
               msgpack_object_kv *const pend = obj->via.map.ptr + obj->via.map.size;
               for (; p < pend; ++p) {
                  std::optional<std::string> key = std::nullopt;
                  // We only do string keys atm..
                  if (p->key.type != MSGPACK_OBJECT_STR) {
                     printf("ERROR: Non-string key types not supported");
                  } else {
                     key = std::make_optional(std::string(p->key.via.str.ptr, p->key.via.str.size));
                  }
                  load_value(&p->val, key, tape);
               }
            }
         }

         ValueStats serialize_loaded_data(uint8_t *output, size_t &len, bool pretty) override {
            ValueStats stats = {0};
            msgpack_sbuffer sbuf;
            msgpack_packer pk;
            msgpack_zone mempool;

            msgpack_sbuffer_init(&sbuf);
            msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

            auto node_itr = _tape.begin();

            while (node_itr != _tape.end()) {
               serialize_value(&pk, node_itr, std::nullopt);
               node_itr++;
            }

            memcpy(output, sbuf.data, sbuf.size);
            stats.serde_bytes += sbuf.size;

            msgpack_sbuffer_destroy(&sbuf);
            
            return stats;
         }

         ValueStats serialize_value(msgpack_packer *pk, TapeIter &node_itr, std::optional<std::string> name) {
            ValueStats stats = {0};

            switch (node_itr->tpe) {
               case MSGPACK_OBJECT_NIL:
                  msgpack_pack_nil(pk);
                  break;
               case MSGPACK_OBJECT_MAP:
                  serialize_map(pk, node_itr);
                  break;
               case MSGPACK_OBJECT_ARRAY:
                  serialize_list(pk, node_itr);
                  break;
               case MSGPACK_OBJECT_STR: {
                  std::string s = std::any_cast<std::string>(node_itr->value.value());
                  msgpack_pack_str_with_body(pk, s.c_str(), s.length());
                  break;
               }
               case MSGPACK_OBJECT_NEGATIVE_INTEGER:
                  msgpack_pack_int64(pk, std::any_cast<int64_t>(node_itr->value.value()));
                  break;
               case MSGPACK_OBJECT_POSITIVE_INTEGER:
                  msgpack_pack_uint64(pk, std::any_cast<uint64_t>(node_itr->value.value()));
                  break;
               case MSGPACK_OBJECT_FLOAT:
                  msgpack_pack_double(pk, std::any_cast<double>(node_itr->value.value()));
                  break;
               case MSGPACK_OBJECT_BOOLEAN:
                  if (std::any_cast<bool>(node_itr->value.value()))
                     msgpack_pack_true(pk);
                  else
                     msgpack_pack_false(pk);
                  break;
               default:
                  printf("ERROR: Attempted serialization of unsupported msgpack type: %x", node_itr->tpe);
                  break;
            }

            return stats;
         }

         ValueStats serialize_list(msgpack_packer *pk, TapeIter &node_itr) {
            ValueStats stats = {0};

            msgpack_pack_array(pk, node_itr->container_size);
            node_itr++;
            while (!node_itr->end_of_container) {
               serialize_value(pk, node_itr, node_itr->field_name);
               node_itr++;
            }

            return stats;
         }

         ValueStats serialize_map(msgpack_packer *pk, TapeIter &node_itr) {
            ValueStats stats = {0};
            msgpack_pack_map(pk, node_itr->container_size);
            node_itr++;
            while (!node_itr->end_of_container) {
               // Key
               msgpack_pack_str(pk, node_itr->field_name->length());
               msgpack_pack_str_body(pk, node_itr->field_name->c_str(), node_itr->field_name->length());

               // Value
               serialize_value(pk, node_itr, node_itr->field_name);
               node_itr++;
            }

            return stats;
         }
   };
}
