from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info(f"Data Quality checking for {table} table")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) == 0 or len(records[0]) == 0 or records[0][0] == 0:
                raise ValueError(f" {table} has no records")