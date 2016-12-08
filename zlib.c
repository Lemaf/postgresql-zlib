#include <postgres.h>
#include <fmgr.h>
#include <utils/bytea.h>
#include <utils/datum.h>
#include <zlib.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum        pg_zlib_decompress(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_zlib_decompress);

Datum
pg_zlib_decompress(PG_FUNCTION_ARGS)
{
    bytea *data;
    bytea *res;
    int expected_rate = 6,
        zlib_res;

    uLongf res_len, data_len;

    data = PG_GETARG_BYTEA_P(0);
    data_len = VARSIZE(data) - VARHDRSZ;

    do {
        res_len = data_len * expected_rate;
        res = palloc(VARHDRSZ + res_len);
        zlib_res = uncompress((Bytef*)VARDATA(res), &res_len, (Bytef*)VARDATA(data), data_len);
        if (zlib_res == Z_BUF_ERROR) {
            expected_rate *= 2;
            pfree(res);
            continue;
        } else if (zlib_res != Z_OK) {
            ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                 errmsg("zlib decompress error: %s", zError(zlib_res))));
        }
        break;
    } while (1);

    SET_VARSIZE(res, VARHDRSZ + res_len);

    PG_FREE_IF_COPY(data, 0);
    PG_RETURN_BYTEA_P(res);
}


Datum        pg_zlib_compress(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_zlib_compress);

Datum
pg_zlib_compress(PG_FUNCTION_ARGS)
{
    bytea *data;
    bytea *res;
    int level;
    int zlib_res;

    uLongf res_len, data_len;

    data = PG_GETARG_BYTEA_P(0);
    level = PG_GETARG_INT32(1);

    if (level < Z_NO_COMPRESSION)
        level = Z_DEFAULT_COMPRESSION;
    else if (level > Z_BEST_COMPRESSION)
        level = Z_BEST_COMPRESSION;

    data_len = VARSIZE(data) - VARHDRSZ;
    res_len = compressBound(data_len);

    res = palloc(VARHDRSZ + res_len);
    zlib_res = compress2((Bytef*)VARDATA(res), &res_len, (Bytef*)VARDATA(data), data_len, level);
    if (zlib_res != Z_OK) {
        ereport(ERROR, 
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("zlib compress error: %s", zError(zlib_res))));
    }

    SET_VARSIZE(res, VARHDRSZ + res_len);
    PG_FREE_IF_COPY(data, 0);
    PG_RETURN_BYTEA_P(res);
}