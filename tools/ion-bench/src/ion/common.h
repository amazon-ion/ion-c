#pragma once
#include <any>
#include <optional>
#include <vector>
#include <numeric>

#include "ionc/ion.h"

#include "../common.h"

namespace ion {

   enum class Format {
      Text,
      Binary,
      JSON,
   };

   constexpr bool is_text(Format f) {
      return f != Format::Binary;
   }

   struct IonData {
      int64_t tpe;
      bool null_container;
      std::optional<std::string> field_name;
      std::optional<std::any> value;
      std::vector<std::string> annotations;
   };

   inline void print_iondata(const IonData &data) {
      printf("[IONDATA] tpe:0x%.4lX field_name:%s",
            data.tpe,
            data.field_name.value_or("none").c_str()
      );
      auto annots = std::accumulate(data.annotations.begin(), data.annotations.end(), std::string(),
            [](const std::string &a, const std::string &b) -> std::string {
               return a + (a.length() > 0 ? "," : "") + b;
            }
      );
      printf(" value:");
      if (!data.value.has_value()) printf("none");
      else {
         if (data.value->type() == typeid(std::string))
            printf("\"%s\"", std::any_cast<std::string>(data.value.value()).c_str());
      }
      printf(" annotations:[%s]\n", annots.c_str());
   }

   struct IonDecimalDeleter {
      void operator()(ION_DECIMAL *d) {
         ion_decimal_free(d);
         free(d);
      }
   };

   typedef std::shared_ptr<ION_DECIMAL> IonDecimalPtr;
   typedef struct { std::string value; } Symbol;
}
