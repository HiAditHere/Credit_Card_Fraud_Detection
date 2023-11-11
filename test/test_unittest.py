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
        self.dagbag = DagBag(dag_folder = 'dags/', include_examples = False)

    def test_number_of_columns(self):

        dag_id = 'pipeline'
        dag = self.dagbag.get_dag(dag_id)

        dag.clear()
        dag.run()

        task_instance = dag.get_task('ohe_task')
        xcom_result = task_instance.xcom_pull(task_ids='ohe_task')
        
        df = pickle.loads(xcom_result)

        #self.assertTrue(len(df) > 0, "DataFrame should not be empty")
        self.assertEqual(df.shape[1], 68, "Number of columns should be 68" )

if __name__ == 'main':
    unittest.main()