import requests

from airflow.hooks.base_hook import BaseHook


class LaunchHook(BaseHook):

    def __init__(self, conn_id=None, api_version=1.4):
        super().__init__(source=None)
        self._conn_id = conn_id
        self._api_version = api_version

        self._conn = None
        self.base_url = None

    def get_conn(self):
        """Initialise and cache session."""
        if self._conn is None:
            self._conn = requests.Session()

            if self._conn_id:
                conn = self.get_connection(self._conn_id)

                if conn.host and "://" in conn.host:
                    self.base_url = conn.host
                else:
                    # schema defaults to HTTP
                    schema = conn.schema if conn.schema else "http"
                    host = conn.host if conn.host else ""
                    self.base_url = schema + "://" + host
            else:
                self.base_url = "https://launchlibrary.net"

        return self._conn

    def get_launches(self, start_date: str, end_date: str):
        """Fetches launches from the API."""

        session = self.get_conn()
        response = session.get(
            f"{self.base_url}/{self._api_version}/launch",
            params={"start_date": start_date, "end_date": end_date},
        )
        response.raise_for_status()
        return response.json()["launches"]
