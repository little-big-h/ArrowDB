#include "../Source/ArrowDB.hpp"
#include "ITTNotifySupport.hpp"
#include <arrow/api.h>
#include <benchmark/benchmark.h>
#include <iostream>
#include <sys/_types/_int64_t.h>

auto const MIN_SIZE = 2048;
auto const MAX_SIZE = 16 * 1024 * 1024;

using namespace std;
using namespace arrow;

static auto const vtune = VTuneAPIInterface{"ArrowDB"};
static void ArrowBenchmark(benchmark::State& state) {
  vtune.startSampling("ArrowBenchmark");
  auto run = 0UL;

  for(auto _ : state) {
    run++;
    arrow::Int64Builder builder;
    builder.Resize(state.range(0));
    vector<int64_t> thing{0, 1, 2, 3, 4, 5, 6, 7};
    thing[0] = run;
    for(auto i = 0u; i < state.range(0) << 3; i++) {
      builder.AppendValues(thing);
    }
    shared_ptr<arrow::Array> a;
    arrow::Status st = builder.Finish(&a);
    benchmark::DoNotOptimize(a);
  }
  vtune.stopSampling();
}
BENCHMARK(ArrowBenchmark)->Range(MIN_SIZE, MAX_SIZE); // NOLINT

static void ArrowBulkBenchmark(benchmark::State& state) {
  vtune.startSampling("ArrowBulkBenchmark");
  auto run = 0UL;
  for(auto _ : state) {
    run++;
    arrow::Int64Builder builder;
    builder.AppendEmptyValues(state.range(0));
    for(auto i = 0u; i < state.range(0); i++) {
      builder[i] = run + i & 7;
    }
    shared_ptr<arrow::Array> a;
    arrow::Status st = builder.Finish(&a);
    benchmark::DoNotOptimize(a);
  }
  vtune.stopSampling();
}
BENCHMARK(ArrowBulkBenchmark)->Range(MIN_SIZE, MAX_SIZE); // NOLINT

static void ArrowBulkUnrolledBenchmark(benchmark::State& state) {
  vtune.startSampling("ArrowBulkUnrolledBenchmark");
  arrow::Int64Builder builder;
  auto run = 0UL;

  for(auto _ : state) {
    run++;

    builder.AppendEmptyValues(state.range(0));
    for(auto i = 0u; i < state.range(0); i += 8) {
      for(auto j = 0u; j < 8; j++) {
        builder[i + j] = run + j;
      }
    }
    shared_ptr<arrow::Array> a;
    arrow::Status st = builder.Finish(&a);
    benchmark::DoNotOptimize(a);
    benchmark::DoNotOptimize(st);
  }
  vtune.stopSampling();
}
BENCHMARK(ArrowBulkUnrolledBenchmark)->Range(MIN_SIZE, MAX_SIZE); // NOLINT

static void VectorBenchmark(benchmark::State& state) {
  vtune.startSampling("VectorBenchmark");
  auto run = 0UL;
  for(auto _ : state) {
    run++;
    vector<int64_t> a;
    a.reserve(state.range(0));
    for(auto i = 0u; i < state.range(0); i++) {
      a.push_back(run + i & 7);
    }
    benchmark::DoNotOptimize(a);
  }
  vtune.stopSampling();
}
BENCHMARK(VectorBenchmark)->Range(MIN_SIZE, MAX_SIZE); // NOLINT

static void CArrayUnrolledBenchmark(benchmark::State& state) {
  vtune.startSampling("CArrayUnrolledBenchmark");
  auto run = 0UL;
  for(auto _ : state) {
    run++;
    auto a = (int64_t*)malloc(state.range(0) * sizeof(int64_t));
    for(auto i = 0u; i < state.range(0); i += 8) {
      for(auto j = 0u; j < 8; j++) {
        a[i + j] = j + run;
      }
    }
    benchmark::DoNotOptimize(a);
    free(a);
  }
  vtune.stopSampling();
}
BENCHMARK(CArrayUnrolledBenchmark)->Range(MIN_SIZE, MAX_SIZE); // NOLINT

BENCHMARK_MAIN();
