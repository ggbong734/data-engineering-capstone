from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 conn_id='',
                 dq_checks='',   
                 *args, **kwargs):
        '''
        params:
        conn_id: (str) redshift connection that is set up in Airflow
        dq_checks: (dict) dictionary with 'check_sql' and 'expected_value' as keys. 'check_sql' is mapped to an sql query string.
                    'expected_value" should be mapped to an integer value. 
        '''
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id      # see Lesson 3 Demo 1: Operator PlugIn Demo   
        self.dq_checks= dq_checks
    
    def execute(self, context):
        '''
        This function runs the data quality sql checks and raise an error if output value is not as expected.
        This function automatically runs when Operator is called.
        '''
        database_hook=PostgresHook(self.conn_id)
        failing_tests=[]
        error_count = 0
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
                                   
            records = database_hook.get_records(sql)[0]
            
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
                
        if error_count > 0:
            self.log.info("Tests failed")
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        
        self.log.info('DataQualityOperator been completed')