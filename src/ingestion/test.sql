COPY INTO bronze_grants
FROM (
    SELECT $1, METADATA$FILENAME, CURRENT_TIMESTAMP()
    FROM @grants_stage_test
)
FILE_FORMAT = grants_json_format