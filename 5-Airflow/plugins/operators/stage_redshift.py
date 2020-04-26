from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'

    sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        COMPUPDATE OFF
        REGION '{}'
        {}
    """

    sql_csv = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 json_path="",
                 file_type="json",
                 region="",
                 extras="",
                 delimiter=",",
                 header=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_path = s3_path
        self.json_path = json_path
        self.file_type = file_type
        self.region = region
        self.extras = extras
        self.aws_credentials_id = aws_credentials_id
        self.delimiter = delimiter
        self.header = header

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.file_type == "json":
            query = StageToRedshiftOperator.sql_json.format(
                self.table,
                self.s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path,
                self.region,
                self.extras
            )

        if self.file_type == "csv":
            query = StageToRedshiftOperator.sql_csv.format(
                self.table,
                self.s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.header,
                self.delimiter
            )
        
        redshift.run(query)
