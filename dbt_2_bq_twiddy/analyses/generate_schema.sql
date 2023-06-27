{{ codegen.generate_base_model(
    source_name='airbyte_bq_test_srandle2',
    table_name='Property',
    materialized=['table', 'view', 'incremental']
    )
}}