import sys
import os
import unittest
from airflow.models import DagBag
import pandas as pd
import pickle

# Get the path to the project's root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

class TestPipeline(unittest.TestCase):

    def sample(self):
        self.assertEqual(1+1,2)

    def setUp(self):
        self.dagbag = DagBag(dag_folder = "/dags", include_examples = False)

    def test_number_of_columns(self):

        print(project_root)

        dag_id = 'pipeline'
        dag = self.dagbag.get_dag(dag_id)

        print(dag)

        if dag:
            dag.clear()

        dag.run()

        task_instance = dag.get_task('ohe_task')
        xcom_result = task_instance.xcom_pull(task_ids='OHE')
        
        df = pickle.loads(xcom_result)

        self.assertTrue(len(df) > 0, "DataFrame should not be empty")
        self.assertEqual(df.shape[1], 68, "Number of columns should be 68" )

    def test_ohe(self):

        dag_id = 'pipeline'
        dag = self.dagbag.get_dag(dag_id)

        if dag:
            dag.clear()

        dag.run()

        task_instance = dag.get_task('ohe_task')
        xcom_result = task_instance.xcom_pull(task_ids='ohe_task')

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