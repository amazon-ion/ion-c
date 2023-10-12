#include "benchmark/benchmark.h"
#include <string.h>
#include <iostream>
#include <map>
#include <functional>

#include <argtable3.h>

#include "ion/ionc.h"
#include "json/yy.h"
#include "json/json-c.h"
#include "msgpack/msgpack.h"
#include "cbor/libcbor.h"
#include "benchmarks.h"

#include "common.h"
#ifdef TRACK_MEMORY
#  include "memory.h"
#endif

Library *libraries[] = {
   new ion::IonC<ion::Format::Binary>(),
   new ion::IonC<ion::Format::Text>(),
   new ion::IonC<ion::Format::JSON>(),
   new yyjson::YYJson(),
   new jsonc::JsonC(),
   new msgpack::MsgPackC(),
   new libcbor::LibCBOR()
};

Library *find_library(char const *l) {
   for (int i=0; i < sizeof(libraries)/sizeof(libraries[0]); i++) {
      if (!strcmp(libraries[i]->name(), l))
         return libraries[i];
   }
   return nullptr;
}

typedef std::function<int(benchmark::State &st, Library*, const char *, bool)> BenchmarkFunc;
std::map<std::string, BenchmarkFunc> benchmarks = {
   { "deserialize_all", deserialize_all() },
   { "serialize_all",   serialize_all() },
};

template <size_t N>
struct data_name {
   char _chars[N+1] = {};

   constexpr data_name(char const *s) {
      for (unsigned i = 0; i < N; ++i)
         _chars[i] = s[i];
   }

   constexpr operator char const*() const { return _chars; }
   constexpr size_t length() const { return N; }
};

template<size_t N>
data_name(char const (&)[N]) -> data_name<N - 1>;

template <typename T>
void deser_profile(const char *const dataset, Format format, size_t num) {
   T impl;
   DataLoader data((char const *)dataset, T::suffix(), format);
   ValueStats stats = {0};

   for (int i=0; i < num; ++i) {
      auto vals = impl.deserialize(data.buffer(), data.length());
      stats += vals;
   }
}

/*
 * Runs the equivalent of deser<..> for an ion dataset specified on the cmdline.
 * This will execute the deserialization a number of times in order to allow
 * an appropriate level of samples to be taken, or to monitor activity in realtime.
 */
int run_for_profiler(int argc, char **argv) {
   namespace fs = std::filesystem;
   fs::path path = argv[1];
   Format format = Format::Binary;
   fs::path dataset = path.filename();

   if (dataset.stem().has_extension()) { // Text format..
      std::string mid = dataset.stem().extension();
      if (mid == ".min") {
         format = Format::Compact;
         dataset = dataset.stem().stem();
      } else if (mid == ".pretty") {
         format = Format::Pretty;
         dataset = dataset.stem().stem();
      } else {
         std::cout << "Unsure of dataset format.\n";
         return 1;
      }
   } else {
      dataset = dataset.stem();
   }

   std::cout << "Deserializing dataset: " << dataset << " (" << ((format == Format::Pretty)?"Pretty":"Compact") << ")\n";

   if (path.extension() == ".10n") { // Binary
      deser_profile<ion::IonC<ion::Format::Binary>>(dataset.c_str(), Format::Binary, 100);
   } else if (path.extension() == ".ion") { // Text
      deser_profile<ion::IonC<ion::Format::Text>>(dataset.c_str(), format, 100);
   } else {
      std::cout << "Unprepared to handle this format.\n";
      return 2;
   }
   return 0;
}

void list_supported_libs() {
   printf("Supported Format Implementations\n");
   for (int i=0; i < sizeof(libraries)/sizeof(libraries[0]); i++) {
      printf("   %s\n", libraries[i]->name());
   }
}

void list_supported_benchmarks() {
   printf("Implemented Benchmarks:\n");
   for (auto i : benchmarks) {
      printf("   %s\n", i.first.c_str());
   }
}

int main(int argc, char** argv) {
   struct arg_lit *help, *list_libs, *list_benchs, *pretty_print, *no_stats;
   struct arg_str *benchmark, *dataset, *lib, *name;
   void *argtable[] = {
      help        = arg_litn(NULL, "help", 0, 1, "Display this help and exit."),
      list_libs   = arg_litn("L", "list-libs", 0, 1, "List available libraries to benchmark."),
      list_benchs = arg_litn("B", "list-bench", 0, 1, "List available benchmarks."),

      name        = arg_str1("n", "name", 0, "Name to use for the run in reporting"),
      benchmark   = arg_str1("b", "benchmark", 0, "Benchmark to run. (read or write)"),
      dataset     = arg_strn("d", "dataset", "FILE", 0, argc+2, "Add a dataset to run benchmark with."), 
      lib         = arg_str1("l", "library", 0, "Library to use (use -L to see a list of supported libraries)"),
      no_stats    = arg_litn(NULL, "no-stats", 0, 1, "Do not generate benchmarks stats. (Used primarily for profiling)"),

      // Options for Text
      pretty_print = arg_litn("p", "pretty-print", 0, 1, "Pretty print text output"),

      arg_end(20),
   };

   int nerrors = arg_parse(argc, argv, argtable);
   if (help->count > 0) {
      printf("Usage: %s\n", argv[0]);
      arg_print_glossary(stdout, argtable, "  %-25s %s\n");
   } else if (list_libs->count > 0) {
      list_supported_libs();
   } else if (list_benchs->count > 0) {
      list_supported_benchmarks();
   } else if (benchmark->count == 1) {
      printf("Benchmark: %s\n", benchmark->sval[0]);
      if (dataset->count == 0) {
         printf("ERROR: must provide a dataset.\n");
         return 1;
      }
      auto bench_func = benchmarks.at(benchmark->sval[0]);
      auto is_pretty = pretty_print->count == 1;

      if (lib->count == 0) {
         printf("ERROR: must provide a format implementation");
         return 1;
      }
      auto library = find_library(lib->sval[0]);

      for (int i=0; i < dataset->count; i++) {
         auto pos = strrchr(dataset->sval[i], '/');
         benchmark::RegisterBenchmark(pos == NULL ? dataset->sval[i] : pos+1, bench_func, library, dataset->sval[i], is_pretty);
      }
      benchmark::Initialize(&argc, argv);
      benchmark::RunSpecifiedBenchmarks();
      benchmark::Shutdown();
   } else {
      printf("Usage: %s\n", argv[0]);
      arg_print_glossary(stdout, argtable, "  %-25s %s\n");
   }
   arg_freetable(argtable, sizeof(argtable)/sizeof(argtable[0]));
   return 0;
}
