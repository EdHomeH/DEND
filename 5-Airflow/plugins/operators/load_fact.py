
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    sql = """
        INSERT INTO {}
        {};
        COMMIT;
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_query = load_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        query = LoadFactOperator.sql.format(
            self.table,
            self.load_query
        )
        
        redshift.run(query)
