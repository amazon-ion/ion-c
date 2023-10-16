
#ifndef __JSON_COMMON_H__
#define __JSON_COMMON_H__

#include <any>
#include <optional>
#include <string>

namespace json {

   template <typename T>
   struct JsonData {
      T tpe;
      bool end_of_container;
      std::optional<std::any> value;
      std::optional<std::string> field_name;

      JsonData(T tpe) : tpe(tpe), end_of_container(false), value(std::nullopt), field_name(std::nullopt) {}
      JsonData &with_field_name(const std::optional<std::string> &name) { field_name = name; return *this; }
      JsonData &with_value(const std::optional<std::any> &val) { value = val; return *this; }
      JsonData that_closes() { end_of_container = true; return *this; }
   };

}

#endif /* __JSON_COMMON_H__ */
