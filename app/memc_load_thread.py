#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache
from queue import Queue
from threading import Thread
from multiprocessing.dummy import Pool as ThreadPool
from functools import wraps, lru_cache
import time

MAX_READ_THREADS = 2
SENTINEL = "###QUIT###"
NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


@lru_cache()
def get_memcache(memc_addr):
    return memcache.Client([memc_addr])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc = get_memcache(memc_addr)
            memc.set(key, packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


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


def stop_workers(workers, queue):
    for worker in workers:
        queue.put(SENTINEL)
    for worker in workers:
        worker.join()


def main_pool(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    log_line_queue = Queue()
    parsed_line_queue = Queue()
    to_cache_queue = Queue()


    fns = glob.glob(options.pattern)
    if not fns:
        logging.info("No logs")
        # stop_workers(all_workers, all_queues)
        return

    pool = ThreadPool(max(len(fns), MAX_READ_THREADS))

    def read_log_to_queue(fn, queue):
        @wraps(fn)
        def wrapper(*args):
            new_args = [*args]
            new_args.append(queue)
            return fn(*new_args)

        return wrapper

    rl = read_log_to_queue(read_log, log_line_queue)

    pool.map(rl, fns)
    pool.close()
    pool.join()

    parsWorkers = []
    for _ in range(1):
        worker = AppsInstalledParseWorker(log_line_queue, parsed_line_queue)
        worker.start()
        parsWorkers.append(worker)

    insertWorkers = []
    for _ in range(1):
        worker = InsertAppsInstalledWorker(device_memc, parsed_line_queue, to_cache_queue, options.dry)
        worker.start()
        insertWorkers.append(worker)

    toCacheWorkers = []
    for _ in range(2):
        worker = LoadToMemCacheWorker(to_cache_queue)
        worker.start()
        toCacheWorkers.append(worker)

    stop_workers(parsWorkers, log_line_queue)
    stop_workers(insertWorkers, parsed_line_queue)
    stop_workers(toCacheWorkers, to_cache_queue)

    processed = sum(w.processed for w in insertWorkers)
    errors = sum(w.errors for w in insertWorkers)

    print(get_memcache.cache_info())

    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))

    # for fn in fns:
    #     dot_rename(fn)


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    for fn in glob.iglob(options.pattern):
        processed = errors = 0
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn)
        for line in fd:
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
            ok = insert_appsinstalled(memc_addr, appsinstalled, options.dry)
            if ok:
                processed += 1
            else:
                errors += 1
        if not processed:
            fd.close()
            dot_rename(fn)
            continue

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename(fn)


def read_log(fn, out_queue):
    logging.info('Processing %s' % fn)
    fd = gzip.open(fn)
    i = 0
    for line in fd:
        i += 1
        if i % 100000 == 0:
            logging.info(f"Read {i} from {fn}")
        if i % 500000 == 0:
            out_queue.put(SENTINEL)
            fd.close()
            return
        line = line.strip()
        line = line.decode("utf8")
        if not line:
            continue
        out_queue.put(line)

    out_queue.put(SENTINEL)
    fd.close()


class ReadLogWorker(Thread):
    def __init__(self, fn, out_queue):
        Thread.__init__(self)
        self.fn = fn
        self.out_queue = out_queue

    def run(self):
        i = 0
        while True:
            logging.info('Processing %s' % self.fn)
            fd = gzip.open(self.fn)
            i = 0
            for line in fd:
                i += 1
                if i % 100000 == 0:
                    logging.info(f"Read {i} from {self.fn}")
                line = line.strip()
                line = line.decode("utf8")
                if not line:
                    continue
                self.out_queue.put(line)

            self.out_queue.put(SENTINEL)
            fd.close()
            dot_rename(fn)
            self.out_queue.put(a)
            self.in_queue.task_done()


class AppsInstalledParseWorker(Thread):
    def __init__(self, in_queue, out_queue):
        Thread.__init__(self)
        self.in_queue = in_queue
        self.out_queue = out_queue

    def run(self):
        i = 0
        while True:
            line = self.in_queue.get()

            if isinstance(line, str) and line == SENTINEL:
                self.in_queue.task_done()
                logging.info(f"AppsInstalledParseWorker {self.name} END")
                break

            i += 1
            if i % 100000 == 0:
                logging.info(f"Parse {i} lines {self.name}")

            line_parts = line.strip().split("\t")
            if len(line_parts) < 5:
                self.in_queue.task_done()
                continue
            dev_type, dev_id, lat, lon, raw_apps = line_parts
            if not dev_type or not dev_id:
                self.in_queue.task_done()
                continue
            try:
                apps = [int(a.strip()) for a in raw_apps.split(",")]
            except ValueError:
                apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
                logging.info("Not all user apps are digits: `%s`" % line)
                self.in_queue.task_done()
                continue
            try:
                lat, lon = float(lat), float(lon)
            except ValueError:
                logging.info("Invalid geo coords: `%s`" % line)
                self.in_queue.task_done()
                continue
            a = AppsInstalled(dev_type, dev_id, lat, lon, apps)
            self.out_queue.put(a)
            self.in_queue.task_done()


class InsertAppsInstalledWorker(Thread):
    def __init__(self, device_memc, in_queue, to_cache_queue, dry_run=False):
        Thread.__init__(self)
        self.device_memc = device_memc
        self.dry_run = dry_run
        self.in_queue = in_queue
        self.to_cache_queue = to_cache_queue
        self.processed = 0
        self.errors = 0

    def run(self):
        i = 0
        while True:
            appsinstalled = self.in_queue.get()

            if isinstance(appsinstalled, str) and appsinstalled == SENTINEL:
                self.in_queue.task_done()
                logging.info(f"InsertAppsInstalledWorker {self.name} END")
                break
            i += 1
            if i % 100000 == 0:
                logging.info(f"Insert {i} apps {self.name}")

            if not appsinstalled:
                self.errors += 1
                self.in_queue.task_done()
                continue

            memc_addr = self.device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                logging.error("Unknow device type: %s" % appsinstalled.dev_type)
                self.errors += 1
                self.in_queue.task_done()
                continue

            ua = appsinstalled_pb2.UserApps()
            ua.lat = appsinstalled.lat
            ua.lon = appsinstalled.lon
            key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
            ua.apps.extend(appsinstalled.apps)
            packed = ua.SerializeToString()
            # @TODO persistent connection
            # @TODO retry and timeouts!
            try:
                if self.dry_run:
                    logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
                else:
                    task = (key, packed, memc_addr)
                    self.to_cache_queue.put(task)
            except Exception as e:
                logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
                self.errors += 1
                self.in_queue.task_done()
                continue

            self.processed += 1
            self.in_queue.task_done()


class LoadToMemCacheWorker(Thread):
    def __init__(self, in_queue):
        Thread.__init__(self)
        self.in_queue = in_queue

    def run(self):
        i = 0
        while True:
            task = self.in_queue.get()

            if isinstance(task, str) and task == SENTINEL:
                self.in_queue.task_done()
                logging.info(f"LoadToMemCacheWorker {self.name} END")
                break

            key, packed, memc_addr = task
            i += 1
            if i % 10000 == 0:
                logging.info(f"Loaded {i} apps {self.name}")

            memc = get_memcache(memc_addr)
            memc.set(key, packed)

            self.in_queue.task_done()

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


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/home/dmitry/Загрузки/logs/20170929000000.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        t = time.time()
        main_pool(opts)
        print(time.time() - t)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
