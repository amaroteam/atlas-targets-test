"""Template ETL Main Entry Point."""
import sys
import time

from citadel.standard import get_standard_configs
from citadel.logging import logging_setup
from task_classes.extraction import Extraction, ExtractionBackfill
from task_classes.extract_validation import ExtractValidationBackfill
from task_classes.transformation import Transformation, TransformationBackfill
from task_classes.transform_validation import TransformValidationBackfill
from task_classes.loading import Loading, LoadingBackfill

log = logging_setup()

"""
   Created At: 14/07/2021
      Contact: data@amaro.com
   Created By: 
Last Reviewed:
  Reviewed by:
Copyright 2012-2021 AMARO. All rights reserved.
"""

class TemplateClass:
    """Template ETL class."""

    def __init__(self, url, url_xl, stage):
        """Init ETL class."""
        self.url = url
        self.url_xl = url_xl
        self.stage = stage
        self.project_name = "company_targets_looker" # here
        self.methods = {
            "extract": Extraction(self.url, self.stage, self.project_name).main,
            "extract_validation": self.pass_method,
            "transform": Transformation(self.url, self.stage, self.project_name).main,
            "transform_validation": self.pass_method,
            "load": Loading(self.url, self.stage, self.project_name).main,
        }
        self.backfill_methods = {
            "extract": ExtractionBackfill(self.url_xl, self.stage, self.project_name).main,
            "extract_validation": ExtractValidationBackfill(self.url, self.stage, self.project_name).main,
            "transform": TransformationBackfill(self.url_xl, self.stage, self.project_name).main,
            "transform_validation": TransformValidationBackfill(self.url, self.stage, self.project_name).main,
            "load": LoadingBackfill(self.url, self.stage, self.project_name).main,
        }

    def get_methods(self):
        """Return methods dict."""
        return self.methods

    def get_backfill_methods(self):
        """Return methods dict."""
        return self.backfill_methods

    def execute_task(self, task, event):
        """Execute Task."""
        event = self.methods[task](event)
        log.info(f"Successfully execution of {task}")
        return event

    def execute_task_backfill(self, task, event):
        """Execute Task Backfill."""
        event = self.backfill_methods[task](event)
        log.info(f"Successfully execution of {task}")
        return event

    def pass_method(self, event):
        return event

def handler(event, context):
    """Entrypoint of Template ETL Process."""
    stage, env, task, mode = event["stage"], event["env"], event["task"], event["mode"]
    tasks = ['extract', 'extract_validation', 'transform', 'transform_validation', 'load', 'end']

    url = get_standard_configs(stage, env)["sfl_transformer_url"]
    url_xl = get_standard_configs(stage, env)["sfl_transformerXL_url"]

    start_time = time.strftime("%X", time.gmtime(time.time()))
    
    etl = TemplateClass(url, url_xl, stage)

    if mode == 'backfill':
        log.info("Backfill start time: " + start_time)
        log.info("Running in {stage}...".format(stage=stage))
        
        #event = etl.execute_task_backfill(task, event)
        #For local testing, remove the following comments and comment the line above and check if __name__ == "__main__":
        event = etl.execute_task_backfill('extract', event)
        # event = etl.execute_task_backfill('extract_validation', event)
        event = etl.execute_task_backfill('transform', event)
        # event = etl.execute_task_backfill('transform_validation', event)
        event = etl.execute_task_backfill('load', event)
    else:
        log.info("Start time: " + start_time)
        log.info("Running in {stage}...".format(stage=stage))

        #event = etl.execute_task(task, event)
        #For local testing, remove the following comments and comment the line above and check if __name__ == "__main__":
        event = etl.execute_task('extract', event)
        # event = etl.execute_task('extract_validation', event)
        # event = etl.execute_task('transform', event)
        # event = etl.execute_task('transform_validation', event)
        # event = etl.execute_task('load', event)

    event['task'] = tasks[tasks.index(event['task']) + 1]
    log.info("Event: \n")
    log.info(event)

    end_time = time.strftime("%X", time.gmtime(time.time()))
    log.info(
        "\n" +
        "Start time: " +
        start_time +
        "\n" +
        "End time: " +
        end_time +
        "\n"
    )

    return event

if __name__ == "__main__":
    #handler(sys.argv, {})
    #For local testing, remove the following comments and comment the line above
     event = {
        "stage": "dev",
        "env": "local",
        "task": "extract",
        "mode": "nobackfill",
        "lambdaName": "dev-amaro-atlas-etl-template-lambda",
        "hasExtraction": True,
        "kwarg": "null",
        "data": {}
     }
     handler(event, {})