MODULES = zlib
EXTENSION = zlib
DATA = zlib--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
