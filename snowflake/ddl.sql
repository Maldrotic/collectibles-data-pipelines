-- Databases
CREATE DATABASE IF NOT EXISTS collectibles;

-- Schemas
USE DATABASE collectibles;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- Warehouses
CREATE WAREHOUSE IF NOT EXISTS collectibles_warehouse
  WAREHOUSE_SIZE = 'XSMALL';

-- Tables
USE SCHEMA raw;

CREATE TABLE IF NOT EXISTS events_backfill (
  id VARCHAR(255) NOT NULL,
  source VARCHAR(255) NOT NULL,
  type VARCHAR(255) NOT NULL,
  payload VARIANT NOT NULL,
  inserted_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS events_stream (
  id VARCHAR(255) NOT NULL DEFAULT UUID_STRING(),
  source VARCHAR(255) NOT NULL,
  type VARCHAR(255) NOT NULL,
  payload VARIANT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- The staging and marts tables are created by dbt, so we wouldn't need to create them here.