## Loading data into memcache using multiple processes
Run from ./app

python memc_load_process_queue.py


optional arguments:

--test - run in test mode

--pattern "path/to/log"- pattern parsed files (/home/logs/*.tsv.gz)

--idfa, --gaid, --adid, --dvid "host:port" memcache server address 

--use_threads - use threads instead processes for loadind to memcache
