from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 fields="",
                 query="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.fields = fields
        self.query = query
        self.append_data = append_data


    def execute(self, context):
        sql = f"""
            insert into {self.table}
            {self.query}
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
            self.log.info("Appending data into destination Fact table")
            redshift.run(sql)
        else:         
            self.log.info("Clearing data from destination Fact table")
            redshift.run("DELETE FROM {}".format(self.table))
            self.log.info("Loading data into destination Fact table")
            redshift.run(sql)
