from airflow.providers.mysql.hooks.mysql import MySqlHook

def pull_forex_data(
        price_type_name = 'mid',
        instrument_name = 'EUR/USD',
        interval_name = 'Minute',
        output_directory = None,
):
    mysql_hook = MySqlHook(mysql_conn_id = 'django')

    sql = f"SELECT ts.timestamp, cs.o, cs.l, cs.h, cs.c, v.volume FROM timeseries_candlestick cs, timeseries_instrument inst, timeseries_interval iv, timeseries_pricetype pt, timeseries_volume v, timeseries_timestamp ts WHERE cs.instrument_id = inst.id AND cs.interval_id = iv.id AND cs.price_type_id = pt.id AND cs.volume_id = v.id AND cs.timestamp_id = ts.id AND pt.name = '%s' AND inst.name = '%s' AND iv.name = '%s' ORDER BY timestamp;"

    pdf = mysql_hook.get_pandas_df(
        sql % (
            price_type_name,
            instrument_name,
            interval_name
        )
    )

    filename = output_directory + '/' + 'NEW_raw_pdf.parquet'
    pdf.to_parquet(filename)
