from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    
    dims_sql_template = """
    INSERT INTO {}
    ({})
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 query="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.query = query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info(f'Truncating Table: {self.destination_table}')
            redshift.run(f'TRUNCATE TABLE {self.destination_table}')
        self.log.info(f'Loading Dim Table: {self.destination_table}')
        dims_sql = LoadDimensionOperator.dims_sql_template.format(
            self.destination_table,
            self.query
        )
        redshift.run(dims_sql)
