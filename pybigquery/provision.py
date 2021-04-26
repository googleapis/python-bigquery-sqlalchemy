import sqlalchemy.testing.provision

DB = "bigquery"


@sqlalchemy.testing.provision.temp_table_keyword_args.for_db(DB)
def _temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["TEMPORARY"]}
