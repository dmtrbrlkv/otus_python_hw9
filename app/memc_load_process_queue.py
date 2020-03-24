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
from functools import wraps, lru_cache
import time

from multiprocessing import Queue, Process, Value

# default for Xeon X3470, 4 cores, 8 threads
READ_PROCESSES = 1
PARSE_PROCESSES = 8
PACK_PROCESSES = 5
LOAD_PROCESSES = 3

MAX_LINES = None
LOG_EVERY = 100000

SENTINEL = "###QUIT###"


@lru_cache()
def get_memcache(memc_addr):
    return memcache.Client([memc_addr])


NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(queue_in, queue_out, device_memc, processed, errors, dry_run=False):
    i = 0
    while True:
        appsinstalled = queue_in.get()
        if isinstance(appsinstalled, str) and appsinstalled == SENTINEL:
            break

        i += 1
        if i % LOG_EVERY == 0:
            logging.info(f"{os.getpid()} - Insert {i}")

        memc_addr = device_memc.get(appsinstalled.dev_type)

        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()
        try:
            if dry_run:
                logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
            else:
                queue_out.put((key, packed, memc_addr))
        except Exception as e:
            logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
            errors.value += 1
        processed.value += 1


def parse_appsinstalled(queue_in, queue_out, errors):
    i = 0
    while True:
        line = queue_in.get()
        if isinstance(line, str) and line == SENTINEL:
            break

        i += 1
        if i % LOG_EVERY == 0:
            logging.info(f"{os.getpid()} - Parse {i}")

        line_parts = line.strip().split("\t")
        if len(line_parts) < 5:
            errors.value += 1
            continue
        dev_type, dev_id, lat, lon, raw_apps = line_parts
        if not dev_type or not dev_id:
            errors.value += 1
            continue
        try:
            apps = [int(a.strip()) for a in raw_apps.split(",")]
        except ValueError:
            apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
            logging.info("Not all user apps are digits: `%s`" % line)
        try:
            lat, lon = float(lat), float(lon)
        except ValueError:
            logging.info("Invalid geo coords: `%s`" % line)
        appsinstalled = AppsInstalled(dev_type, dev_id, lat, lon, apps)
        queue_out.put(appsinstalled)


def read_log(fn, queue_out):
    fd = gzip.open(fn)
    i = 0
    for line in fd:
        i += 1
        if i % LOG_EVERY == 0:
            logging.info(f"{os.getpid()} - Read {i}")
        if MAX_LINES and (i % MAX_LINES == 0):
            logging.info(f"{os.getpid()} - Read end, {i} processed")
            fd.close()
            break
        line = line.strip()
        line = line.decode("utf8")
        if not line:
            continue
        queue_out.put(line)


def load_to_memcache(queue_in, errors):
    i = 0
    while True:
        task = queue_in.get()

        if isinstance(task, str) and task == SENTINEL:
            logging.info(f"{os.getpid()} - Load end, {i} processed")
            break

        key, packed, memc_addr = task
        i += 1
        if i % LOG_EVERY == 0:
            logging.info(f"{os.getpid()} - Load {i}")

        ok = False
        trys = 10
        while not ok and trys > 0:
            try:
                memc = get_memcache(memc_addr)
                if not memc.set(key, packed):
                    logging.info(f"{os.getpid()} - Not loaded, retry")
                    time.sleep(1)
                    trys -= 1
                else:
                    ok = True
            except Exception as e:
                logging.exception(f"Load error: {e}")
                time.sleep(1)
                trys -= 1

        if not ok:
            logging.info(f"{os.getpid()} - Not loaded, skip")
            errors.value += 1


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    for fn in glob.iglob(options.pattern):
        logging.info(f"Start process file {fn}")

        processed = Value("d", 0)
        errors = Value("d", 0)

        parse_q = Queue()
        pack_q = Queue()
        load_q = Queue()

        read_ps = []
        for i in range(options.read_p):
            p = Process(target=read_log, args=(fn, parse_q), name=f"Read {i}")
            p.start()
            read_ps.append(p)

        parse_ps = []
        for i in range(options.parse_p):
            p = Process(target=parse_appsinstalled, args=(parse_q, pack_q, errors), name=f"Parse {i}")
            p.start()
            parse_ps.append(p)

        pack_ps = []
        for i in range(options.pack_p):
            p = Process(target=insert_appsinstalled, args=(pack_q, load_q, device_memc, processed, errors, options.dry),
                        name=f"Pack {i}")
            p.start()
            pack_ps.append(p)

        load_ps = []
        for i in range(options.load_p):
            p = Process(target=load_to_memcache, args=(load_q, errors), name=f"Load {i}")
            p.start()
            load_ps.append(p)

        for p in read_ps:
            p.join()
            logging.info(f"read process {p.pid} end")

        for p in parse_ps:
            parse_q.put(SENTINEL)
        for p in parse_ps:
            p.join()
            logging.info(f"parse process {p.pid} end")

        for p in pack_ps:
            pack_q.put(SENTINEL)
        for p in pack_ps:
            p.join()
            logging.info(f"pack process {p.pid} end")

        for p in load_ps:
            load_q.put(SENTINEL)
        for p in load_ps:
            p.join()
            logging.info(f"load process {p.pid} end")


        if not processed.value:
            dot_rename(fn)
            continue

        logging.info(f"End process file {fn}. Processed {processed.value}, errors {errors.value}")

        err_rate = float(errors.value) / processed.value
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfully load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))

        dot_rename(fn)


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
    op.add_option("--read_p", action="store", default=READ_PROCESSES)
    op.add_option("--parse_p", action="store", default=PARSE_PROCESSES)
    op.add_option("--pack_p", action="store", default=PACK_PROCESSES)
    op.add_option("--load_p", action="store", default=LOAD_PROCESSES)

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
        logging.info(f"Run time {time.time() - t}")

    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
