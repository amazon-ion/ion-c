#pragma once

#include "../common.h"
#include "json_object.h"
#include <stdint.h>
#include <optional>
#include <any>
#include <vector>
#include <json.h>

#include "common.h"

namespace jsonc {

   class JsonC : public Library {
      typedef json::JsonData<enum json_type> JsonData;
      protected:
         std::vector<JsonData> _tape;
         size_t _data_size;
      public:
         const char *name() const override { return "json-c"; }

         static constexpr char const *suffix() {
            return "json";
         }

         size_t data_size() const override {
            return _data_size;
         }

         ValueStats deserialize(uint8_t *input, size_t in_size) override {
            ValueStats stats = {0};
            enum json_tokener_error jerr;

            json_tokener *tok = json_tokener_new();
            json_object *doc = json_tokener_parse_ex(tok, (const char *)input, in_size);
            jerr = json_tokener_get_error(tok);

            if (doc != nullptr && jerr == json_tokener_success) {
               auto valstats = process_value(doc);
               stats += valstats;
               json_object_put(doc);
            } else {
                printf("ERROR: %s\n", json_tokener_error_desc(jerr));
            }

            json_tokener_free(tok);
            return stats;
         }

         ValueStats process_value(json_object *val) {
            ValueStats stats = {0};

            switch (json_object_get_type(val)) {
               case json_type_null:
                  stats.num_nulls++;
                  break;
               case json_type_boolean: {
                  bool b = json_object_get_boolean(val);
                  stats.num_bools++;
                  break;
               }
               case json_type_double: {
                  double d = json_object_get_double(val);
                  stats.num_nums++;
                  break;
               }
               case json_type_int: {
                  int64_t i = json_object_get_int64(val);
                  stats.num_nums++;
                  break;
               }
               case json_type_string: {
                  const char *str = json_object_get_string(val);
                  std::string val(str);
                  stats.str_size += val.length();
                  stats.num_strs++;
                  break;
               }
               case json_type_object: {
                  auto valstats = process_object(val);
                  stats += valstats;
                  stats.num_objs++;
                  break;
               }
               case json_type_array: {
                  auto valstats = process_list(val);
                  stats += valstats;
                  stats.num_lists++;
                  break;
               }
            }

            return stats;
         }

         ValueStats process_object(json_object *obj) {
            ValueStats stats = {0};

            json_object_iterator iter = json_object_iter_begin(obj);
            json_object_iterator end = json_object_iter_end(obj);

            while (!json_object_iter_equal(&iter, &end)) {
               std::string name(json_object_iter_peek_name(&iter));
               stats.str_size += name.length();

               json_object *val = json_object_iter_peek_value(&iter);
               auto vstats = process_value(val);

               stats += vstats;

               json_object_iter_next(&iter);
            }

            return stats;
         }

         ValueStats process_list(json_object *list) {
            ValueStats stats = {0};

            int arrlen = json_object_array_length(list);
            for (int i=0; i<arrlen; ++i) {
               auto val = json_object_array_get_idx(list, i);
               auto vstats = process_value(val);

               stats += vstats;
            }
            return stats;
         }

         void load_data(uint8_t *input, size_t size) override {
            enum json_tokener_error jerr;
            _tape.clear();

            json_tokener *tok = json_tokener_new();
            json_object *doc = json_tokener_parse_ex(tok, (const char *)input, size);
            jerr = json_tokener_get_error(tok);

            if (doc != nullptr && jerr == json_tokener_success) {
               load_value(std::nullopt, doc, _tape);
               json_object_put(doc);
            } else {
               printf("ERROR: %s\n", json_tokener_error_desc(jerr));
            }
            json_tokener_free(tok);
            _data_size = size;
         }

         void load_value(std::optional<std::string> name, json_object *val, std::vector<JsonData> &tape) {
            switch (json_object_get_type(val)) {
               case json_type_null:
                  tape.push_back(JsonData(json_type_null).with_field_name(name));
                  break;
               case json_type_boolean: {
                  bool b = json_object_get_boolean(val);
                  tape.push_back(JsonData(json_type_boolean)
                     .with_field_name(name)
                     .with_value(std::make_optional(std::make_any<bool>(b)))
                  );
                  break;
               }
               case json_type_double: {
                  double d = json_object_get_double(val);
                  tape.push_back(JsonData(json_type_double)
                     .with_field_name(name)
                     .with_value(std::make_optional(std::make_any<double>(d)))
                  );
                  break;
               }
               case json_type_int: {
                  int i = json_object_get_int(val);
                  tape.push_back(JsonData(json_type_int)
                     .with_field_name(name)
                     .with_value(std::make_optional(std::make_any<int64_t>(i)))
                  );
                  break;
               }
               case json_type_string: {
                  const char *str = json_object_get_string(val);
                  std::string s(str);
                  tape.push_back(JsonData(json_type_string)
                     .with_field_name(name)
                     .with_value(std::make_optional(std::make_any<std::string>(s)))
                  );
                  break;
               }
               case json_type_object:
                  tape.push_back(JsonData(json_type_object).with_field_name(name));
                  load_object(val, tape);
                  tape.push_back(JsonData(json_type_object).with_field_name(name).that_closes());
                  break;
               case json_type_array:
                  tape.push_back(JsonData(json_type_array).with_field_name(name));
                  load_list(val, tape);
                  tape.push_back(JsonData(json_type_array).with_field_name(name).that_closes());
                  break;
            }
         }

         void load_list(json_object *list, std::vector<JsonData> &tape) {
            int arrlen = json_object_array_length(list);
            for (int i=0; i < arrlen; i++) {
               auto val = json_object_array_get_idx(list, i);
               load_value(std::nullopt, val, tape);
            }
         }

         void load_object(json_object *obj, std::vector<JsonData> &tape) {
            json_object_iterator iter = json_object_iter_begin(obj);
            json_object_iterator end = json_object_iter_end(obj);

            while (!json_object_iter_equal(&iter, &end)) {
               std::string name(json_object_iter_peek_name(&iter));
               json_object *val = json_object_iter_peek_value(&iter);

               load_value(name, val, tape);

               json_object_iter_next(&iter);
            }
         }

         ValueStats serialize_loaded_data(uint8_t *output, size_t &len, bool pretty) override {
            ValueStats stats = {0};
            auto node_itr = _tape.begin();
            auto root = serialize_value(node_itr);
            size_t output_len = 0;

            auto str = json_object_to_json_string_length(root, (pretty) ? JSON_C_TO_STRING_PRETTY:JSON_C_TO_STRING_PLAIN, &output_len);
            size_t to_write = std::min(output_len, len - 1);
            memcpy(output, str, to_write);
            output[to_write + 1] = 0x0;
            len = to_write + 1;

            stats.serde_bytes += len;

            json_object_put(root);
            return stats;
         }

         json_object *serialize_value(std::vector<JsonData>::iterator &node_itr) {
            json_object *new_obj = nullptr;

            switch (node_itr->tpe) {
               case json_type_array: {
                  new_obj = json_object_new_array();
                  serialize_list(new_obj, ++node_itr);
                  break;
               }
               case json_type_object: {
                  new_obj = json_object_new_object();
                  serialize_object(new_obj, ++node_itr);
                  break;
               }
               case json_type_int: {
                  new_obj = json_object_new_int64(std::any_cast<int64_t>(node_itr->value.value()));
                  break;
               }
               case json_type_string: {
                  std::any s = node_itr->value.value();
                  new_obj = json_object_new_string(std::any_cast<std::string>(s).c_str());
                  break;
               }
               case json_type_double: {
                  new_obj = json_object_new_double(std::any_cast<double>(node_itr->value.value()));
                  break;
               }
               case json_type_boolean: {
                  new_obj = json_object_new_boolean(std::any_cast<bool>(node_itr->value.value()));
                  break;
               }
               case json_type_null:
                  new_obj = json_object_new_null();
                  break;
            }
            return new_obj;
         }

         void serialize_list(json_object *list, std::vector<JsonData>::iterator &node_itr) {
            while (!node_itr->end_of_container) {
               auto obj = serialize_value(node_itr);
               json_object_array_add(list, obj);
               node_itr++;
            }
         }

         void serialize_object(json_object *parent, std::vector<JsonData>::iterator &node_itr) {
            while (!node_itr->end_of_container) {
               auto obj = serialize_value(node_itr);
               json_object_object_add(parent, node_itr->field_name->c_str(), obj);
               node_itr++;
            }
         }
   };

}
