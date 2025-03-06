
import pandas as pd

def generate_offset_map(
        previous_output_filename,
        table_prefix = 'timezone_added', # get this somewhere else
        table_prefix_new = 'weekday_hour_shifted',
):

    hour_list = []
    weekday_list = []
    shifted_list = []

    shifted_list.extend([0] * 17)

    for i in range(0, 4):
        weekday_list.extend([i] * 24)
        hour_list.extend(sorted(list(range(0, 24))))

        if i >= 1:
            shifted_list.extend([i] * 24)

    shifted_list.extend([4] * 24)

    weekday_list.extend([4] * 17)
    hour_list.extend(sorted(list(range(0, 17))))

    weekday_list.extend([6] * 7)
    hour_list.extend(sorted(list(range(17, 24))))
    shifted_list.extend([0] * 7)

    pdf_shifted_weekday_manually_constructed = pd.DataFrame({'weekday_tz' : weekday_list, 'hour_tz' : hour_list, 'weekday_shifted' : shifted_list})

    #filename_and_path = str(candlestick_data_dict['initial_candlesticks_pdf_full_output_path']).replace(table_prefix, table_prefix_new)

    filename_and_path = previous_output_filename.replace(table_prefix, table_prefix_new)
        
    pdf_shifted_weekday_manually_constructed.to_parquet(filename_and_path)

    to_return = {'shifted_candlesticks_pdf' : pdf_shifted_weekday_manually_constructed, 'shifted_candlesticks_pdf_full_output_path' : filename_and_path}

    return to_return

        
