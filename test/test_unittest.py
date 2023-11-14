import sys
import os
import unittest
from airflow.models import DagBag, TaskInstance, DagRun
import pandas as pd
import pickle
import time
from airflow.utils.db import create_session
from datetime import datetime
from airflow.api.common.experimental.trigger_dag import trigger_dag

# Get the path to the project's root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

class TestPipeline(unittest.TestCase):

    def sample(self):
        self.assertEqual(1+1,2)

    def setUp(self):
        self.dagbag = DagBag(dag_folder = "/home/runner/work/Credit_Card_Fraud_Detection/Credit_Card_Fraud_Detection/dags", include_examples = False)

    def test_number_of_columns(self):

        print(project_root)

        dag_id = 'pipeline'
        dag = self.dagbag.get_dag(dag_id)

        if 'pipeline' in self.dagbag.dags:

            time.sleep(30)

            # Trigger the DAG run
            execution_date = datetime(2023-11-11)
            run_id = f"manual__{execution_date}"

            # Create a DagRun manually
            dag_run = DagRun(
                dag_id=dag.dag_id,
                run_id=run_id,
                execution_date=execution_date
            )

            # Add the DagRun to the session and commit
            dag_run.dag = dag
            dag_run.verify_integrity()
            dag_run.update_state()
            dag_run.update_state()
            dag_run.log = []
            dag_run.start_date = execution_date
            dag_run.run_id = run_id
            dag_run.state = 'running'

            dag_run.create()

            # Wait for the DAG run to complete
            #dag_run = DagRun.find(dag_id=dag_id, execution_date=execution_date)
            dag_run.wait_until_finished()

            #task_instance = TaskInstance(task = dag.get_task('OHE'), execution_date = datetime.now())
            task_instance = dag_run.get_task_instance(task_id='OHE')

            xcom_result = task_instance.xcom_pull()
            
            df = pickle.loads(xcom_result)

            self.assertTrue(len(df) > 0, "DataFrame should not be empty")
            self.assertEqual(df.shape[1], 68, "Number of columns should be 68" )

        else:

            print("No such DAG ******************************************")

    def test_ohe(self):

        dag_id = 'pipeline'
        dag = self.dagbag.get_dag(dag_id)

        if dag:
            dag.clear()

        dag.run()

        task_instance = dag.get_task('OHE')

        with create_session() as session:
            xcom_result = task_instance.xcom_pull(task_ids='ohe_task', session = session)

        df = pickle.loads(xcom_result)

        def is_one_hot_encoded(column):
            unique_values = column.unique()
            if set(unique_values) == {0, 1}:
                if 1 in unique_values:
                    if column.sum() == len(column):
                        return True
                    
        male = is_one_hot_encoded(df['gender_M'])
        female = is_one_hot_encoded(df['gender_F'])

        self.assertEqual(male, True)
        self.assertEqual(female, True)

if __name__ == 'main':
    unittest.main()