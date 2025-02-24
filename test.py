import pandas as pd


if False:
    root_directory = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components/output/queries'

    file_id = '1f09ecbb-2d83-46c6-9e9c-195792519cb6'

    df_initial = pd.read_parquet(root_directory + '/candlestick_query_results_' + file_id + '.parquet')

    df_tz = pd.read_parquet(root_directory + '/timezone_added_' + file_id + '.parquet')

    df_shift = pd.read_parquet(root_directory + '/weekday_hour_shifted_' + file_id + '.parquet')

    df_merge = pd.read_parquet(root_directory + '/merge_by_pandas_' + file_id + '.parquet')

    df_shifted = pd.read_parquet(root_directory + '/shifted_' + file_id + '.parquet')

    df_exploded = pd.read_parquet('/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components/output/queries/spark_exploded_9754759d-2884-4612-8f32-35e6687b7a16.parquet')


    


    print()
    print(df_initial)
    print()
    print(df_shift)
    print()
    print(df_tz)
    print()
    print(df_merge)
    print()
    print(df_shifted)
    print()
    print(df_exploded)
    print()


if True:
    run_id = '309457bc-a227-4332-8c0b-2cf5dd38749c'
    run_dir = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components/output/queries'

    full_exploded_output_path = run_dir + '/spark_exploded_' + run_id + '.parquet'

    full_scaled_output_path = run_dir + '/spark_scaled_' + run_id + '.parquet'

    full_scaling_stats_path = run_dir + '/spark_scaling_stats_' + run_id + '.parquet'
    
    df_exploded = pd.read_parquet(full_exploded_output_path)
    df_scaled = pd.read_parquet(full_scaled_output_path)
    df_scaling_stats = pd.read_parquet(full_scaling_stats_path)

    print()
    print(df_exploded)
    print()
    print(df_scaled)
    print()
    print(df_scaled.columns)
    print()
    print(df_scaling_stats)
    print()
    print(df_scaling_stats.columns)
    print()
    
    




