#
# define query to run
#
def get_candlestick_pull_query():
    sql_query_for_candlestick_pull = """SELECT

    ts.timestamp, cs.o, cs.l, cs.h, cs.c, v.volume

    FROM

    timeseries_candlestick cs, timeseries_instrument inst,
    timeseries_interval iv, timeseries_pricetype pt,
    timeseries_volume v, timeseries_timestamp ts

    WHERE

    cs.instrument_id = inst.id
    AND cs.interval_id = iv.id
    AND cs.price_type_id = pt.id
    AND cs.volume_id = v.id
    AND cs.timestamp_id = ts.id

    AND pt.name = '%s'
    AND inst.name = '%s'
    AND iv.name = '%s'

    ORDER BY timestamp
    ;
    """

    return sql_query_for_candlestick_pull
