from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 dq_check="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.dq_check = dq_check

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        for check in self.dq_check:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            self.log.info(f"running {sql}")
            
            records = redshift_hook.get_records(sql)
            num_records = records[0][0]

            if int(num_records) != int(exp_result):
                raise ValueError(f"Data quality check failed. Got {num_records} rows, but expected {exp_result}")
            self.log.info(f"Data quality check passed!")