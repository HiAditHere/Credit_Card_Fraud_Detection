import sys
import os
import unittest
from airflow.models import DagBag
import pandas as pd

# Get the path to the project's root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

class TestPipeline(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(dag_folder = "dags/", include_examples = False)
        
    def test_task_count(self):
        """Check task count of pipeline dag"""
        dag_id='pipeline2'
        dag = self.dagbag.get_dag(dag_id)
        self.assertEqual(len(dag.tasks), 6)  
        
    def test_dependencies_of_drop_task(self):
        """Check the task dependencies of drop_task in pipeline dag"""
        dag_id='pipeline2'
        dag = self.dagbag.get_dag(dag_id)
        drop_task = dag.get_task('drop_task')


        upstream_task_ids = list(map(lambda task: task.task_id, drop_task.upstream_list))
        self.assertListEqual(upstream_task_ids, ['load_data_task'])
        downstream_task_ids = list(map(lambda task: task.task_id, drop_task.downstream_list))
        self.assertListEqual(downstream_task_ids, ['merge_category_task'])
  