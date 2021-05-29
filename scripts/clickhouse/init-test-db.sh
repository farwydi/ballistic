#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    CREATE DATABASE test;
    CREATE TABLE test.table_1 (
      record_time DateTime,
      string_val String,
      bool_var Int8,
      int_32_val Int32,
      u_int_64_val UInt64,
      int_val Int32,
      float_32_val Float32,
      float_64_val Float64
    ) ENGINE = Memory;

    CREATE TABLE test.table_2 (
      record_time DateTime,
      string_val String,
      bool_var Int8,
      int_32_val Int32,
      u_int_64_val UInt64,
      int_val Int32,
      float_32_val Float32,
      float_64_val Float64
    ) ENGINE = Memory;
EOSQL