#pragma once
#include "common.h"
#include "benchmark/benchmark.h"

// A deserialization benchmark that materializes every item in the dataset.
struct deserialize_all {
   int operator()(benchmark::State &st, Library *l, const char *dataset, bool pretty) {
      size_t total_bytes = 0;
      ValueStats stats = {0};
      DataLoader data(dataset);

      for (auto _ : st) {
         auto vals = l->deserialize(data.buffer(), data.length());
         total_bytes += data.length();
         stats += vals;
      }
      st.counters["Bps"] = benchmark::Counter(total_bytes, benchmark::Counter::kIsRate);
      st.counters["objs"] = benchmark::Counter(stats.num_objs);
      st.counters["bools"] = benchmark::Counter(stats.num_bools);
      st.counters["strs"] = benchmark::Counter(stats.num_strs);
      st.counters["nulls"] = benchmark::Counter(stats.num_nulls);
      st.counters["nums"] = benchmark::Counter(stats.num_nums);
      return 0;
   }
};

// A serialization benchmark that first reads the dataset into an in-memory format, and then
// serializes everything into a new representation.
struct serialize_all {
   int operator()(benchmark::State &st, Library *l, const char *dataset, bool pretty) {
      ValueStats stats = {0};
      DataLoader data(dataset);

      st.SetLabel(l->name());

      l->load_data(data.buffer(), data.length());
      size_t buffer_size = l->data_size() * 2;

      // TODO: Let the implementation determine this.
      uint8_t *buffer = new uint8_t[buffer_size];
      size_t output_size = 0;

      for (auto _ : st) {
         output_size = buffer_size;
         auto iter_stats = l->serialize_loaded_data(buffer, output_size, pretty);
         stats += iter_stats;
      }

      delete[] buffer;

      st.counters["Bps"] = benchmark::Counter(stats.serde_bytes, benchmark::Counter::kIsRate, benchmark::Counter::OneK::kIs1024);
      st.counters["objs"] = benchmark::Counter(stats.num_objs);
      st.counters["bools"] = benchmark::Counter(stats.num_bools);
      st.counters["strs"] = benchmark::Counter(stats.num_strs);
      st.counters["nulls"] = benchmark::Counter(stats.num_nulls);
      st.counters["nums"] = benchmark::Counter(stats.num_nums);
      return 0;
   }
};
