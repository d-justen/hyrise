./$1/hyriseJoinOrderingEvaluator --evaluation=baseline --cost-model=linear --dynamic-timeout-plan=false --timeout-query=0 --timeout-plan=0 --max-plan-execution-count=1 --max-plan-generation-count=1 --workload=job --imdb-dir=/home/Moritz.Eyssen/imdb/csv --job-dir=/home/Moritz.Eyssen/join-order-benchmark/ --cardinality-estimation=cache-only --cardinality-estimator-execution-timeout=85 --save-query-iterations-results=true --save-plan-results=true --iterations-per-query=5 --visualize=true --cardinality-estimation-cache-access=ro --cardinality-estimation-cache-path=joe/cardinality_estimation_cache.production2.json --isolate-queries=false --cost-sample-dir=joe/cost-sample -- $2
