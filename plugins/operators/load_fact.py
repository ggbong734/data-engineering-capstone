from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 conn_id='',
                 sql = '',
                 target_table = '',
                 *args, **kwargs):
        '''
        params:
        conn_id: (str) redshift connection that is set up in Airflow
        sql: (str) sql query for inserting data into target table
        target_table: (str) target table on Redshift database to insert data into 
        '''
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.target_table = target_table
        self.sql = sql

    def execute(self, context):
        '''
        This function connects to the Redshift database and then insert data into the target fact table.
        This function automatically runs when Operator is called.
        '''
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        insert_query = "INSERT INTO {} \n".format(self.target_table) + self.sql
        redshift.run(insert_query)
        self.log.info('LoadFactOperator is completed. New data appended to {}.'.format(self.target_table))
