#!/usr/bin/env python3

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

from paradicms_etl.extractors.google_sheets_extractor import GoogleSheetsExtractor
from paradicms_etl.pipeline import Pipeline
from paradicms_etl.transformers.spreadsheet_transformer import SpreadsheetTransformer
from paradicms_ssg.git_hub_action import GitHubAction


class Action(GitHubAction):
    @dataclass(frozen=True)
    class RequiredInputs(GitHubAction.RequiredInputs):
        spreadsheet_id: str

    @dataclass(frozen=True)
    class Inputs(GitHubAction.OptionalInputs, RequiredInputs):
        pass

    def __init__(self, *, inputs: Inputs, temp_dir_path: Path):
        GitHubAction.__init__(
            self,
            optional_inputs=inputs,
            required_inputs=inputs,
            temp_dir_path=temp_dir_path,
        )
        self.__inputs = inputs

    @classmethod
    def main(cls, inputs: Optional[Inputs] = None):
        if inputs is None:
            inputs = cls.Inputs.from_args()
        with TemporaryDirectory() as temp_dir:
            cls(inputs=inputs, temp_dir_path=Path(temp_dir)).__main()

    def __main(self):
        Pipeline(
            extractor=GoogleSheetsExtractor(
                extracted_data_dir_path=self._extracted_data_dir_path,
                spreadsheet_id=self.__inputs.spreadsheet_id,
            ),
            id=self.__inputs.pipeline_id,
            loader=self._create_loader(),
            transformer=SpreadsheetTransformer(pipeline_id=self.__inputs.pipeline_id),
        ).extract_transform_load()


if __name__ == "__main__":
    Action.main()
