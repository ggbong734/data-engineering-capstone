from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageParquetToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 table = '',
                 redshift_conn_id='',
                 aws_credentials_id='',
                 s3_bucket ='',
                 s3_key='',
                 iam_role_arn='',
                 *args, **kwargs):
        '''
        params:
        table: (str) target table on Redshift database to copy data into         
        redshift_conn_id: (str) redshift connection that is set up in Airflow
        aws_credentials_id: (str) aws connection that is set up in Airflow
        s3_bucket: (str) name of s3 bucket without the "s3://" prefix
        s3_key: (str) name of s3 bucket key/subfolder. Wildcard such as *.json is not allowed. Only specify subdirectory.
        file_type: (str) format of files in S3. The two supported types are 'csv' or 'parquet'.
        '''
        super(StageParquetToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.s3_key=s3_key
        self.s3_bucket = s3_bucket
        self.iam_role_arn = iam_role_arn


    def execute(self, context):
        '''
        This function copies JSON data from the S3 bucket into the target table in Redshift.
        This function runs automatically when operator is called. 
        '''
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Clearing data from destination Redshift table')
        redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('Copying data from S3 to Redshift')
        rendered_key=self.s3_key.format(**context)
        s3_path='s3://{}/{}'.format(self.s3_bucket, rendered_key)     
        
        copy_sql = """
        COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        FORMAT AS PARQUET;
        """
        formatted_sql = copy_sql.format(
            self.table,
            s3_path,
            self.iam_role_arn
        )
        redshift.run(formatted_sql)
        





