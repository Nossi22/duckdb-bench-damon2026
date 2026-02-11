#!/bin/bash

sudo tmpfs-create
rm /mnt/ramdisk/*
cp data/tpch-10/* /mnt/ramdisk

mkdir -p measurements/queries
./build/queries -s parquet -t 1
./build/queries -s memory -t 1

mkdir -p measurements/throughput
./build/throughput -s filtered -t 8 -S 8
./build/throughput -s memory -t 8-32 -S 8
./build/throughput -s parquet -t 8-32 -S 8
