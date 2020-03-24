#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import time
from optparse import OptionParser
from multiprocessing import Process, Pipe, Queue
import os

# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache
from functools import wraps, lru_cache
# from queue import Queue
from threading import Thread
SENTINEL = "###QUIT###"

N_PROCESS = 1
TIMEOUT = 0


def inf_cicle_gen(iter):
    i = 0
    while True:
        yield iter[i]
        i += 1
        if i >= len(iter):
            i = 0


def get_next_conn(parent_conns):
    return inf_cicle_gen(parent_conns)


memcache_cache = {}
def get_memcache(memc_addr):
    if not memc_addr in memcache_cache:
        memcache_cache[memc_addr] = memcache.Client([memc_addr])
        logging.info(f"add to memcache_cache {memc_addr} from {os.getpid()}")
    return memcache_cache[memc_addr]


def load_to_memcache(child_conn_to_cache, memc):
    i = 0
    while True:
        if child_conn_to_cache.poll(TIMEOUT):
            task = child_conn_to_cache.recv()

            if isinstance(task, str) and task == SENTINEL:
                logging.info(f"load_to_memcache {os.getpid()} END, {i} proceesed")
                break

            key, packed, memc_addr = task
            i += 1
            if i % 10000 == 0:
                logging.info(f"Loaded {i} apps {os.getpid()}")

            memc =

            ok = False
            trys = 10
            while not ok or trys > 0:
                if not memc.set(key, packed):
                    time.sleep(1)
                    trys -= 1
                else:
                    ok = True
            if not ok:
                logging.info(f"Not loaded")


NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, parent_conn_to_cache, dry_run=False):
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
            task = (key, packed, memc_addr)
            parent_conn_to_cache.send(task)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled_process(queue, parent_conn_to_packed, device_memc):
    i = 0
    while True:
        # if child_conn_line.poll(TIMEOUT):
        #     line = child_conn_line.recv()

        line = queue.get()

        if isinstance(line, str) and line == SENTINEL:
            logging.info(f"parse_appsinstalled_process {os.getpid()} END, {i} proceesed")
            break

        i += 1
        if i % 10000 == 0:
            logging.info(f"Parse {i} lines {os.getpid()}")

        line_parts = line.strip().split("\t")
        if len(line_parts) < 5:
            continue
        dev_type, dev_id, lat, lon, raw_apps = line_parts
        if not dev_type or not dev_id:
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
        if not appsinstalled:
            # errors += 1
            continue
        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            # errors += 1
            logging.error("Unknow device type: %s" % appsinstalled.dev_type)
            continue

        parent_conn_to_packed.send((appsinstalled, memc_addr))


def insert_appsinstalled_process(child_conn_to_packed, parent_conns, dry_run=False):
    i = 0
    next_conn = get_next_conn(parent_conns)
    while True:
        if child_conn_to_packed.poll(TIMEOUT):
            task = child_conn_to_packed.recv()


            if isinstance(task, str) and task == SENTINEL:
                logging.info(f"insert_appsinstalled_process {os.getpid()} END, {i} proceesed")
                break

            i += 1
            if i % 10000 == 0:
                logging.info(f"Insert {i} lines {os.getpid()}")

            appsinstalled, memc_addr = task
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
                    task = (key, packed, memc_addr)
                    parent_conn_to_cache = next(next_conn)
                    parent_conn_to_cache.send(task)
            except Exception as e:
                logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
            #     return False
            # return True


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





def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }


    for fn in glob.iglob(options.pattern):

        processes = []
        parent_conns = []

        for _ in range(N_PROCESS):
            parent_conn_to_cache, child_conn_to_cache = Pipe()
            to_cache_process = Process(target=load_to_memcache, args=(child_conn_to_cache,))
            to_cache_process.start()
            processes.append(to_cache_process)
            parent_conns.append(parent_conn_to_cache)

        next_conn = get_next_conn(parent_conns)

        processed = errors = 0
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn)
        i = 0
        for line in fd:
            i+=1
            if i % 100000 == 0:
                logging.info(f"Read {i} from {fn}")
            if i % 500000 == 0:
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
            parent_conn_to_cache = next(next_conn)
            ok = insert_appsinstalled(memc_addr, appsinstalled, parent_conn_to_cache, options.dry)

            if ok:
                processed += 1
            else:
                errors += 1

        for parent_conn_to_cache in parent_conns:
            parent_conn_to_cache.send(SENTINEL)
        for to_cache_process in processes:
            to_cache_process.join()

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
        # dot_rename(fn)


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
        main_full_proc(opts)
        print(time.time()-t)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
