#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
from threading import Thread
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache
from functools import lru_cache
import time

from multiprocessing import Queue, Process, Value

MAX_LINES = 500000
LOG_EVERY = 100000

SENTINEL = "###QUIT###"

CACHE_SIZE = 1000


@lru_cache()
def get_memcache(memc_addr):
    return memcache.Client([memc_addr])


NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


task_cache = {}
def add_to_queue_with_cache(queues_dict, key, task, cache_size=CACHE_SIZE):
    if not key in task_cache:
        task_cache[key] = []
    tasks = task_cache[key]
    tasks.append(task)

    if cache_size is None or len(tasks) >= cache_size:
        queue = queues_dict[key]
        # while tasks:
        #     task = tasks.pop(0)
        #     queue.put(task)
        queue.put(tasks)
        task_cache[key] = []


def flush_queue_cache(queues_dict, key):
    if not key in task_cache:
        return
    tasks = task_cache[key]
    queue = queues_dict[key]
    # while tasks:
    #     task = tasks.pop(0)
    #     queue.put(task)
    queue.put(tasks)
    task_cache[key] = []


def serialize_appsinstalled_to_task(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()

    if dry_run:
        logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        return None
    else:
        task = (key, packed)
        return task


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def serialize_file_data_process(fn, device_memc, load_queues, processed_v, errors_v, dry):
    processed = errors = 0

    logging.info(f"{os.getpid()} - Start process file {fn}")
    fd = gzip.open(fn)
    i = 0
    for line in fd:
        i += 1
        if i % LOG_EVERY == 0:
            logging.info(f"{os.getpid()} ({fn}) - Read {i}")
        if MAX_LINES and (i % MAX_LINES == 0):
            logging.info(f"{os.getpid()} ({fn}) - Read end, {i} processed")
            fd.close()
            break
        line = line.strip()
        line = line.decode("utf8")
        if not line:
            continue
        appsinstalled = parse_appsinstalled(line)

        if not appsinstalled:
            errors += 1
            continue
        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            errors += 1
            logging.error("Unknow device type: %s" % appsinstalled.dev_type)
            continue

        task = serialize_appsinstalled_to_task(memc_addr, appsinstalled, dry)
        if task:
            add_to_queue_with_cache(load_queues, appsinstalled.dev_type, task)
        if task or dry:
            processed += 1
        else:
            errors += 1

    for key in load_queues:
        flush_queue_cache(load_queues, key)

    fd.close()

    with processed_v.get_lock():
        processed_v.value = processed_v.value + processed

    with errors_v.get_lock():
        errors_v.value = errors_v.value + errors


def load_to_memcache(memc_addr, queue, errors_v, name):
    memc = get_memcache(memc_addr)
    errors = 0
    i = 0
    keep_work = True
    while keep_work:
        tasks = queue.get()
        for task in tasks:
            if isinstance(task, str) and task == SENTINEL:
                logging.info(f"{name} - Load end, {i} processed")
                keep_work = False
                break

            key, packed = task
            i += 1
            if i % LOG_EVERY == 0:
                logging.info(f"{name} - Load {i}")

            ok = False
            trys = 10
            while not ok and trys > 0:
                try:
                    if not memc.set(key, packed):
                        logging.info(f"{name} - Not loaded, retry")
                        time.sleep(1)
                        trys -= 1
                    else:
                        ok = True
                except Exception as e:
                    logging.exception(f"Load error: {e}")
                    time.sleep(1)
                    trys -= 1

            if not ok:
                logging.info(f"{name} - Not loaded, skip")
                errors += 1


    with errors_v.get_lock():
        errors_v.value = errors_v.value + errors


def load_to_memcache_process(memc_addr, queue, errors_v):
    load_to_memcache(memc_addr, queue, errors_v, f"{os.getpid()} ({memc_addr})")


class LoadToMemCache(Thread):
    def __init__(self, memc_addr, queue, errors_v):
        super().__init__()
        self.memc_addr = memc_addr
        self.memc = get_memcache(memc_addr)
        self.queue = queue
        self.errors_v = errors_v

    def run(self):
        load_to_memcache(self.memc_addr, self.queue, self.errors_v, f"{self.name} ({self.memc_addr})")


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    processed = Value("i", 0)
    errors = Value("i", 0)

    load_queues = {}
    load_processes = []
    for dev_type, addr in device_memc.items():
        q = Queue()
        load_queues[dev_type] = q
        if opts.use_threads:
            t = LoadToMemCache(addr, q, errors)
            load_processes.append(t)
            t.start()
        else:
            p = Process(target=load_to_memcache_process, args=(addr, q, errors))
            load_processes.append(p)
            p.start()

    serialize_processes = []

    fns = list(glob.iglob(options.pattern))

    for fn in fns:
        p = Process(target=serialize_file_data_process, args=(fn, device_memc, load_queues, processed, errors, options.dry))
        serialize_processes.append(p)
        p.start()

    for p in serialize_processes:
        p.join()
        logging.info(f"serialize process {p.pid} end")

    for dev, q in load_queues.items():
        q.put([SENTINEL])

    for p in load_processes:
        p.join()
        if opts.use_threads:
            logging.info(f"load thread {p.name} end")
        else:
            logging.info(f"load process {p.pid} end")

    logging.info(f"Processed {processed.value}, errors {errors.value}")
    if not processed.value:
        logging.info(f"Not processed")
        return

    err_rate = float(errors.value) / processed.value
    if err_rate < NORMAL_ERR_RATE:
        for fn in fns:
            dot_rename(fn)
        logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


def value_in_memcache_test(opts):
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    fn = "test.tsv.gz"
    with gzip.open(fn, mode="w") as f:
        f.write(sample.encode("utf8"))

    device_memc = {
        "idfa": opts.idfa,
        "gaid": opts.gaid,
        "adid": opts.adid,
        "dvid": opts.dvid,
    }

    opts.pattern = fn
    main(opts)

    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        memc = get_memcache(device_memc[dev_type])
        value = memc.get(f"{dev_type}:{dev_id}")
        assert value == packed


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/home/dmitry/Загрузки/logs/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("--use_threads", action="store_true", default=False)

    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        value_in_memcache_test(opts)
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        t = time.time()
        main(opts)
        logging.info(f"Run time {time.time() - t:.2f} seconds")

    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
