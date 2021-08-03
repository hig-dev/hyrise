echo "Build plugin"
cmake --build ./cmake-build-release --target hyriseSharedDictionariesPlugin --

echo "Build benchmark"
cmake --build ./cmake-build-release --target hyriseBenchmarkJoinOrder --
export HYRISE_BENCH_BIN="./cmake-build-release/hyriseBenchmarkJoinOrder"

echo "Verify"
export SHARED_DICTIONARIES_PLUGIN="./cmake-build-release/lib/libhyriseSharedDictionariesPlugin.so"
$HYRISE_BENCH_BIN --verify -r 1 --enable_dictionary_sharing > output_verify.log

echo "Run base benchmark"
unset SHARED_DICTIONARIES_PLUGIN
$HYRISE_BENCH_BIN --output bench_result_base.json -r 100 > output_base.log

# Reset plugin path
export SHARED_DICTIONARIES_PLUGIN="./cmake-build-release/lib/libhyriseSharedDictionariesPlugin.so"

# Run all benchmarks
export JACCARD_INDEX_THRESHOLD=0.01
echo "Run $JACCARD_INDEX_THRESHOLD bench"
$HYRISE_BENCH_BIN --output bench_result_$JACCARD_INDEX_THRESHOLD.json -r 100 > output_$JACCARD_INDEX_THRESHOLD.log

export JACCARD_INDEX_THRESHOLD=0.05
echo "Run $JACCARD_INDEX_THRESHOLD bench"
$HYRISE_BENCH_BIN --output bench_result_$JACCARD_INDEX_THRESHOLD.json -r 100 > output_$JACCARD_INDEX_THRESHOLD.log

export JACCARD_INDEX_THRESHOLD=0.1
echo "Run $JACCARD_INDEX_THRESHOLD bench"
$HYRISE_BENCH_BIN --output bench_result_$JACCARD_INDEX_THRESHOLD.json -r 100 > output_$JACCARD_INDEX_THRESHOLD.log

export JACCARD_INDEX_THRESHOLD=0.2
echo "Run $JACCARD_INDEX_THRESHOLD bench"
$HYRISE_BENCH_BIN --output bench_result_$JACCARD_INDEX_THRESHOLD.json -r 100 > output_$JACCARD_INDEX_THRESHOLD.log

export JACCARD_INDEX_THRESHOLD=0.3
echo "Run $JACCARD_INDEX_THRESHOLD bench"
$HYRISE_BENCH_BIN --output bench_result_$JACCARD_INDEX_THRESHOLD.json -r 100 > output_$JACCARD_INDEX_THRESHOLD.log

export JACCARD_INDEX_THRESHOLD=0.4
echo "Run $JACCARD_INDEX_THRESHOLD bench"
$HYRISE_BENCH_BIN --output bench_result_$JACCARD_INDEX_THRESHOLD.json -r 100 > output_$JACCARD_INDEX_THRESHOLD.log

export JACCARD_INDEX_THRESHOLD=0.5
echo "Run $JACCARD_INDEX_THRESHOLD bench"
$HYRISE_BENCH_BIN --output bench_result_$JACCARD_INDEX_THRESHOLD.json -r 100 > output_$JACCARD_INDEX_THRESHOLD.log

export JACCARD_INDEX_THRESHOLD=0.6
echo "Run $JACCARD_INDEX_THRESHOLD bench"
$HYRISE_BENCH_BIN --output bench_result_$JACCARD_INDEX_THRESHOLD.json -r 100 > output_$JACCARD_INDEX_THRESHOLD.log

export JACCARD_INDEX_THRESHOLD=0.7
echo "Run $JACCARD_INDEX_THRESHOLD bench"
$HYRISE_BENCH_BIN --output bench_result_$JACCARD_INDEX_THRESHOLD.json -r 100 > output_$JACCARD_INDEX_THRESHOLD.log

export JACCARD_INDEX_THRESHOLD=0.8
echo "Run $JACCARD_INDEX_THRESHOLD bench"
$HYRISE_BENCH_BIN --output bench_result_$JACCARD_INDEX_THRESHOLD.json -r 100 > output_$JACCARD_INDEX_THRESHOLD.log

export JACCARD_INDEX_THRESHOLD=0.9
echo "Run $JACCARD_INDEX_THRESHOLD bench"
$HYRISE_BENCH_BIN --output bench_result_$JACCARD_INDEX_THRESHOLD.json -r 100 > output_$JACCARD_INDEX_THRESHOLD.log

echo "Benchmarks finished"