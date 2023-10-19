#pragma once
#include <vector>
#include <any>
#include <optional>
#include <utility>

#include "../common.h"

#include "yyjson.h"

#include "common.h"

namespace yyjson {

class YYJson : public Library {
   typedef json::JsonData<std::pair<yyjson_type, yyjson_subtype>> JsonData;
   std::pair<yyjson_type, yyjson_subtype> make_type(yyjson_type tpe, yyjson_subtype sub) {
      return std::make_pair(tpe, sub);
   }
   std::pair<yyjson_type, yyjson_subtype> make_type(yyjson_type tpe) {
      return std::make_pair(tpe, YYJSON_SUBTYPE_NONE);
   }

   protected:
      size_t _data_size;
      std::vector<JsonData> _tape;

   public:
      const char *name() const override { return "yyjson"; }

      static constexpr char const *suffix() {
         return "json";
      }

      size_t data_size() const override {
         return _data_size;
      }

      ValueStats deserialize(uint8_t *input, size_t in_size) override {
         yyjson_read_err err;
         auto doc = yyjson_read_opts((char *)input, in_size, 0, NULL, &err);
         ValueStats stats = {0};

         if (doc != nullptr) {
            auto val = yyjson_doc_get_root(doc);
            auto valstats = process_value(val);
            stats += valstats;
         } else {
             printf("Error: %s at position %ld\n", err.msg, err.pos);
         }

         yyjson_doc_free(doc);
         return stats;
      }

      ValueStats process_value(yyjson_val *val) {
         ValueStats stats = {0};
         switch (yyjson_get_type(val)) {
            case YYJSON_TYPE_OBJ: {
               stats.num_objs++;
               auto objstats = process_object(val);
               stats += objstats;
               break;
            }
            case YYJSON_TYPE_STR: {
               stats.num_strs++;
               const char *s = yyjson_get_str(val);
               std::string str(s, strlen(s));
               stats.str_size += str.length();
               break;
            }
            case YYJSON_TYPE_NUM: {
               stats.num_nums++;
               switch (yyjson_get_subtype(val)) {
                  uint64_t i; int64_t j; double f;
                  case YYJSON_SUBTYPE_REAL:
                     f = yyjson_get_real(val);
                     break;
                  case YYJSON_SUBTYPE_UINT:
                     i = yyjson_get_uint(val);
                     break;
                  case YYJSON_SUBTYPE_SINT:
                     j = yyjson_get_sint(val);
                     break;
               }
               break;
            }
            case YYJSON_TYPE_NULL:
               stats.num_nulls++;
               break;
            case YYJSON_TYPE_ARR: {
               stats.num_lists++;
               auto valstats = process_list(val);
               stats += valstats;
               break;
            }
            case YYJSON_TYPE_BOOL:
               stats.num_bools++;
               break;
         }
         return stats;
      }

      ValueStats process_object(yyjson_val *obj) {
         ValueStats stats = {0};
         yyjson_val *key, *val;
         yyjson_obj_iter iter;
         yyjson_obj_iter_init(obj, &iter);

         while ((key = yyjson_obj_iter_next(&iter))) {
            val = yyjson_obj_iter_get_val(key);
            auto valstats = process_value(val);
            stats += valstats;
         }
         return stats;
      }

      ValueStats process_list(yyjson_val *list) {
         ValueStats stats = {0};

         yyjson_val *val;
         yyjson_arr_iter iter;
         yyjson_arr_iter_init(list, &iter);
         while ((val = yyjson_arr_iter_next(&iter))) {
            auto valstats = process_value(val);
            stats += valstats;
         }

         return stats;
      }

      void load_data(uint8_t *input, size_t in_size) override {
         yyjson_read_err err;
         auto doc = yyjson_read_opts((char *)input, in_size, 0, NULL, &err);
         _tape.clear();

         if (doc != nullptr) {
            auto val = yyjson_doc_get_root(doc);
            load_value(std::nullopt, val, _tape);
         } else {
            printf("ERROR: %s at position %ld\n", err.msg, err.pos);
         }
         _data_size = in_size;
         yyjson_doc_free(doc);
      }

      void load_value(std::optional<std::string> name, yyjson_val *val, std::vector<JsonData> &tape) {
         switch (yyjson_get_type(val)) {
            case YYJSON_TYPE_OBJ: {
               tape.push_back(JsonData(make_type(YYJSON_TYPE_OBJ)).with_field_name(name));
               load_object(val, tape);
               tape.push_back(JsonData(make_type(YYJSON_TYPE_OBJ)).with_field_name(name).that_closes());
               break;
            }
            case YYJSON_TYPE_ARR:
               tape.push_back(JsonData(make_type(YYJSON_TYPE_ARR)).with_field_name(name));
               load_list(val, tape);
               tape.push_back(JsonData(make_type(YYJSON_TYPE_ARR)).with_field_name(name).that_closes());
               break;
            case YYJSON_TYPE_STR: {
               const char *s = yyjson_get_str(val);
               if (s != nullptr) {
                  std::string str(s);
                  tape.push_back(JsonData(make_type(YYJSON_TYPE_STR)).with_field_name(name).with_value(std::make_optional(std::make_any<std::string>(str))));
               }
               break;
            }
            case YYJSON_TYPE_NUM: {
               switch (yyjson_get_subtype(val)) {
                  uint64_t i; int64_t j; double f;
                  case YYJSON_SUBTYPE_REAL:
                     f = yyjson_get_real(val);
                     tape.push_back(JsonData(make_type(YYJSON_TYPE_NUM, YYJSON_SUBTYPE_REAL))
                           .with_field_name(name)
                           .with_value(std::make_optional(std::make_any<double>(f)))
                     );
                     break;
                  case YYJSON_SUBTYPE_UINT:
                     i = yyjson_get_uint(val);
                     tape.push_back(JsonData(make_type(YYJSON_TYPE_NUM, YYJSON_SUBTYPE_UINT))
                           .with_field_name(name)
                           .with_value(std::make_optional(std::make_any<uint64_t>(i)))
                     );
                     break;
                  case YYJSON_SUBTYPE_SINT:
                     j = yyjson_get_sint(val);
                     tape.push_back(JsonData(make_type(YYJSON_TYPE_NUM, YYJSON_SUBTYPE_SINT))
                           .with_field_name(name)
                           .with_value(std::make_optional(std::make_any<int64_t>(j)))
                     );
               }
               break;
            }
            case YYJSON_TYPE_NULL:
               tape.push_back(JsonData(make_type(YYJSON_TYPE_NULL)).with_field_name(name));
               break;
            case YYJSON_TYPE_BOOL: {
               bool b = yyjson_get_bool(val);
               tape.push_back(JsonData(make_type(YYJSON_TYPE_BOOL)).with_field_name(name).with_value(std::make_optional(std::make_any<bool>(b))));
               break;
            }
               
         }
      }

      void load_object(yyjson_val *obj, std::vector<JsonData> &tape) {
         yyjson_val *key, *val;
         yyjson_obj_iter iter;
         yyjson_obj_iter_init(obj, &iter);

         while ((key = yyjson_obj_iter_next(&iter))) {
            std::string key_str(yyjson_get_str(key));
            val = yyjson_obj_iter_get_val(key);
            load_value(key_str, val, tape);
         }
      }

      void load_list(yyjson_val *list, std::vector<JsonData> &tape) {
         yyjson_val *val;
         yyjson_arr_iter iter;
         yyjson_arr_iter_init(list, &iter);
         while ((val = yyjson_arr_iter_next(&iter))) {
            load_value(std::nullopt, val, tape);
         }
      }

      ValueStats serialize_loaded_data(uint8_t *output, size_t &len, bool pretty) override {
         ValueStats stats = {0};
         yyjson_write_err err;
         size_t ser_len;

         auto node_itr = _tape.begin();
         yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
         auto val = serialize_value(node_itr, doc);
         yyjson_mut_doc_set_root(doc, val);

         auto str = yyjson_mut_write_opts(doc, (pretty) ? YYJSON_WRITE_PRETTY : YYJSON_WRITE_NOFLAG, nullptr, &ser_len, &err);
         if (str == nullptr) {
            printf("ERROR generating string: %s\n", err.msg);
         }
         len = std::min(len, ser_len);
         memcpy(output, str, len);
         output[len] = 0;

         stats.serde_bytes += len;
         yyjson_mut_doc_free(doc);
         free(str);

         return stats;
      }

      yyjson_mut_val *serialize_value(std::vector<JsonData>::iterator &node_itr, yyjson_mut_doc *doc) {
         yyjson_mut_val *new_obj = nullptr;

         switch (node_itr->tpe.first) {
            case YYJSON_TYPE_OBJ: {
               new_obj = yyjson_mut_obj(doc);
               serialize_object(doc, new_obj, ++node_itr);
               break;
            }
            case YYJSON_TYPE_ARR: {
               new_obj = yyjson_mut_arr(doc);
               serialize_list(doc, new_obj, ++node_itr);
               break;
            }
            case YYJSON_TYPE_BOOL: {
               new_obj = yyjson_mut_bool(doc, std::any_cast<bool>(node_itr->value.value()));
               break;
            }
            case YYJSON_TYPE_NUM: {
               switch (node_itr->tpe.second) {
                  case YYJSON_SUBTYPE_SINT:
                     new_obj = yyjson_mut_sint(doc, std::any_cast<int64_t>(node_itr->value.value()));
                     break;
                  case YYJSON_SUBTYPE_UINT:
                     new_obj = yyjson_mut_uint(doc, std::any_cast<uint64_t>(node_itr->value.value()));
                     break;
                  case YYJSON_SUBTYPE_REAL:
                     new_obj = yyjson_mut_uint(doc, std::any_cast<double>(node_itr->value.value()));
                     break;
               }
               break;
            }
            case YYJSON_TYPE_STR: {
               std::string s = std::any_cast<std::string>(node_itr->value.value());
               new_obj = yyjson_mut_strcpy(doc, s.c_str());
               break;
            }
         }
         return new_obj;
      }

      void serialize_object(yyjson_mut_doc *doc, yyjson_mut_val *obj, std::vector<JsonData>::iterator &node_itr) {
         while (!node_itr->end_of_container) {
            auto val = serialize_value(node_itr, doc);
            if (node_itr->field_name.has_value()) {
               std::string s = node_itr->field_name.value();
               auto key = yyjson_mut_strcpy(doc, s.c_str());
               yyjson_mut_obj_add(obj, key, val);
            } else {
               printf("ERROR: Field in object does not have a value.\n");
            }
            node_itr++;
         }
      }

      yyjson_mut_val *serialize_list(yyjson_mut_doc *doc, yyjson_mut_val *list, std::vector<JsonData>::iterator &node_itr) {
         while (!node_itr->end_of_container) {
            auto val = serialize_value(node_itr, doc);
            if (!yyjson_mut_arr_append(list, val)) {
               printf("ERROR adding item to list.\n");
            }
            node_itr++;
         }
         return nullptr;
      }
};


}
