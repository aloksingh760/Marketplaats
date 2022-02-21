def read_csv(
        spark, file_location=None, header='true',
        multiLine='true', ignoreLeadingWhiteSpace='true',
        ignoreTrailingWhiteSpace='true', sep=',', encoding='UTF-8', quote='"',
        escape='\\', nullValue='',
        timestampFormat='yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX',
        dateFormat='yyyy-MM-dd', inferSchema='false', schema=None,
        mode='PERMISSIVE'):
    df_file_load = None
    if file_location is None or file_location.strip() == '':
        raise Exception("Invalid File location. Terminating the job.")

    options = {
            'header': header,
            'inferSchema': inferSchema,
            'multiLine': multiLine,
            'ignoreLeadingWhiteSpace': ignoreLeadingWhiteSpace,
            'ignoreTrailingWhiteSpace': ignoreTrailingWhiteSpace,
            'sep': sep,
            'encoding': encoding,
            'quote': quote,
            'escape': escape,
            'nullValue': nullValue,
            'timestampFormat': timestampFormat,
            'dateFormat': dateFormat,
            'mode': mode
            }
    # schema needs to be a valid StructType or a valid string or None
    # if schema is an empty string, don't pass that on to the read function
    if schema:
        options['schema'] = schema
    df_file_load = spark.read.load(path=file_location, format='csv', **options)
        # Assuming that schema is comma separated list of column names
    # if schema:
    #     df_file_load = _apply_schema(df_file_load, schema)

    return df_file_load