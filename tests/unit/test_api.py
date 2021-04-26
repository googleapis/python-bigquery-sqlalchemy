import mock


def test_dry_run():

    with mock.patch("pybigquery._helpers.create_bigquery_client") as create_client:
        import pybigquery.api

        client = pybigquery.api.ApiClient("/my/creds", "mars")
        create_client.assert_called_once_with(
            credentials_path="/my/creds", location="mars"
        )
        client.dry_run_query("select 42")
        [(name, args, kwargs)] = create_client.return_value.query.mock_calls
        job_config = kwargs.pop("job_config")
        assert (name, args, kwargs) == ("", (), {"query": "select 42"})
        assert job_config.dry_run
        assert not job_config.use_query_cache
