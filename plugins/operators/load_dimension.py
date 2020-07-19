from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 conn_id='',
                 sql = '',
                 target_table = '',
                 truncate_first=False, # truncate-load or append-only, default is append-only
                 *args, **kwargs):
        '''
        params:
        conn_id: (str) redshift connection that is set up in Airflow
        sql: (str) sql query for inserting data into target table
        target_table: (str) target table on Redshift database to insert data into 
        truncate_first: (bool) to determine if the target table should be truncated first (delete all data) before insert.
        '''
        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.target_table = target_table
        self.sql = sql
        self.truncate_first = truncate_first
        

    def execute(self, context):
        '''
        This function instructs the operator to connect to the Redshift database, truncate the table if this
        option is selected, and then insert data into the target dimension table.
        This function automatically runs when Operator is called.
        '''
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        if self.truncate_first:
            redshift.run('TRUNCATE {}'.format(self.target_table))
        insert_query = "INSERT INTO {} \n".format(self.target_table) + self.sql
        redshift.run(insert_query)
        self.log.info('LoadDimensionOperator is completed. Data added to table {}.'.format(self.target_table))
