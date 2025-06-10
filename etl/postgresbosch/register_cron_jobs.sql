SELECT cron.schedule(
    'refresh_ctrlx_signals_every_4min',
    '*/4 * * * *',
    $$ SELECT sandbox.refresh_ctrlx_signals(); $$
);

SELECT cron.schedule(
    'pivot_100ms', '*/5 * * * *',
    $$ 
    SELECT sandbox.initialize_columns_for_frequency('100mS');
    SELECT sandbox.initialize_tmp_columns_for_frequency('100mS');
    SELECT sandbox.pivot_ctrlx_data('100mS');
    SELECT sandbox.sync_ctrlx_pivot_table('100mS');
    $$
);
-- SELECT cron.schedule(
--     'pivot_500ms', '*/5 * * * *',
--     $$ 
--     SELECT sandbox.initialize_columns_for_frequency('500mS');
--     SELECT sandbox.initialize_tmp_columns_for_frequency('500mS');
--     SELECT sandbox.pivot_ctrlx_data('500mS');
--     SELECT sandbox.sync_ctrlx_pivot_table('500mS');
--     $$
-- );

SELECT cron.schedule(
    'pivot_1S', '*/5 * * * *',
    $$ 
    SELECT sandbox.initialize_columns_for_frequency('1S');
    SELECT sandbox.initialize_tmp_columns_for_frequency('1S');
    SELECT sandbox.pivot_ctrlx_data('1S');
    SELECT sandbox.sync_ctrlx_pivot_table('1S');
    $$
);

SELECT cron.schedule(
    'pivot_1M', '*/5 * * * *',
    $$ 
    SELECT sandbox.initialize_columns_for_frequency('1M');
    SELECT sandbox.initialize_tmp_columns_for_frequency('1M');
    SELECT sandbox.pivot_ctrlx_data('1M');
    SELECT sandbox.sync_ctrlx_pivot_table('1M');
    $$
);
