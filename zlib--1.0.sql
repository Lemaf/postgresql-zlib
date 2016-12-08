CREATE FUNCTION zlib_decompress(bytea)
RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_zlib_decompress'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION zlib_compress(bytea, integer=-1)
RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_zlib_compress'
LANGUAGE C IMMUTABLE STRICT;
