#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <any>
#include <vector>

#include <filesystem>
#include <typeinfo>

enum Format {
   Pretty,
   Compact,
   Binary
};

class DataLoader {
   private:
      uint8_t *_buffer;
      size_t _length;
   public:
      DataLoader(char const *full_path);
      DataLoader(char const *path, char const *suffix, Format format);
      ~DataLoader();
      void load_data(char const *full_path);
      uint8_t *buffer() const { return _buffer; }
      size_t length() const { return _length; }
};

struct ValueStats {
   uint64_t num_objs;
   uint64_t num_bools;
   uint64_t num_strs;
   uint64_t num_nulls;
   uint64_t num_nums;
   uint64_t num_lists;
   uint64_t str_size;
   uint64_t serde_bytes;

   void operator+=(ValueStats &other) {
      num_objs += other.num_objs;
      num_bools += other.num_bools;
      num_strs += other.num_strs;
      num_nulls += other.num_nulls;
      num_nums += other.num_nums;
      num_lists += other.num_lists;
      str_size += other.str_size;
      serde_bytes += other.serde_bytes;
   }
};


template <typename T>
struct TapeData {
   T tpe;
   bool end_of_container;
   size_t container_size;   // Used only for msgpack currently.
   std::optional<std::any> value;
   std::optional<std::string> field_name;

   TapeData(T tpe) : tpe(tpe), end_of_container(false), value(std::nullopt), field_name(std::nullopt) {}
   TapeData &with_field_name(const std::optional<std::string> &name) { field_name = name; return *this; }
   TapeData &with_value(const std::optional<std::any> &val) { value = val; return *this; }
   TapeData &with_container_size(size_t s) { container_size = s; return *this; }
   TapeData that_closes() { end_of_container = true; return *this; }
};

template <typename T>
using Tape = std::vector<TapeData<T>>;

template <typename T>
using TapeItr = typename Tape<T>::iterator;

class Library {
   public:
      virtual const char *name() const = 0;
      virtual ValueStats deserialize(uint8_t *input, size_t in_size) = 0;
      virtual void load_data(uint8_t *input, size_t in_size) = 0;
      virtual size_t data_size() const = 0;
      virtual ValueStats serialize_loaded_data(uint8_t *output, size_t &len, bool pretty) = 0;
};

// Very basic typelist for holding our registered library implementations.
template <typename... types>
struct typelist {};

template <template<typename T> typename func, typename head>
void for_list(typelist<head> &list) {
   func<head> f;
   f();
}

template <template<typename T> class func, typename first, typename second, typename... rest>
void for_list(typelist<first, second, rest...> &list) {
   func<first> f;
   f();
   typelist<second, rest...> remaining;
   for_list<func>(remaining);
}

template <template<typename T> class func, typename first, typename second, typename... rest>
void list_until(typelist<first, second, rest...> &list) {
}
