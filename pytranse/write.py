def write_csv(df, spark, file_location=None,single_file=False,
              header=True, delimiter=',', quote='"', escape='\\',
              null_value='null', date_format='yyyy-MM-dd HH:mm:ss.S',
              codec='None', quote_mode='MINIMAL', mode='append'):
    if not file_location or (isinstance(file_location, str) and
                             not file_location.strip()):
        raise Exception("Invalid File location : " + file_location +
                        ". Terminating the job.")
    if not df:
        raise Exception("Invalid Dataframe : " + df +
                        ". Terminating the job.")

    if not spark:
        raise Exception("Please call data_studio.setup_data_studio.setup "
                        "before using components")

   

    if single_file:
        df = df.coalesce(1)

    (df.write
     .format("csv")
     .mode(mode)
     .options(
         header=header,
         sep=delimiter,
         quote=quote,
         escape=escape,
         nullValue=null_value,
         dateFormat=date_format,
         quoteMode=quote_mode,
         compression=codec)
     .save(file_location))