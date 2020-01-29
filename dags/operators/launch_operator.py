import json
import os
import tempfile

from airflow.models import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.decorators import apply_defaults

from hooks.launch_hook import LaunchHook


class LaunchToGcsOperator(BaseOperator):
    template_fields = ("_start_date", "_end_date", "_output_path")

    @apply_defaults
    def __init__(self, start_date, output_bucket, output_path, end_date=None,
                 launch_conn_id=None, gcp_conn_id="google_cloud_default", **kwargs):
        super().__init__(**kwargs)
        self._output_bucket = output_bucket
        self._output_path = output_path

        self._start_date = start_date
        self._end_date = end_date

        self._launch_conn_id = launch_conn_id
        self._gcp_conn_id = gcp_conn_id

    def execute(self, context):
        self.log.info("Fetching launch data")
        launch_hook = LaunchHook(conn_id=self._launch_conn_id)
        result = launch_hook.get_launches(
            start_date=self._start_date,
            end_date=self._end_date
        )
        self.log.info("Fetched data for %d launches", len(result))

        self.log.info(
            "Uploading data to gcs://%s/%s", self._output_bucket, self._output_path
        )
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self._gcp_conn_id
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = os.path.join(tmp_dir, "result.json")
            with open(tmp_path, "w") as file_:
                json.dump(result, file_)

            gcs_hook.upload(
                bucket=self._output_bucket,
                object=self._output_path,
                filename=tmp_path
            )

        # gcs_hook.upload(
        #     bucket=self._output_bucket,
        #     object=self._output_path,
        #     data=result
        # )
