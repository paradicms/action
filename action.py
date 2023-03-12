#!/usr/bin/env python3

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional
from urllib.parse import urlparse

from paradicms_etl.extractor import Extractor
from paradicms_etl.extractors.excel_2010_extractor import Excel2010Extractor
from paradicms_etl.extractors.google_sheets_extractor import GoogleSheetsExtractor
from paradicms_etl.pipeline import Pipeline
from paradicms_etl.transformers.spreadsheet_transformer import SpreadsheetTransformer
from paradicms_ssg.git_hub_action import GitHubAction
from paradicms_ssg.models.root_model_classes_by_name import ROOT_MODEL_CLASSES_BY_NAME


class Action(GitHubAction):
    @dataclass(frozen=True)
    class RequiredInputs(GitHubAction.RequiredInputs):
        spreadsheet: str

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
    def main(cls):
        with TemporaryDirectory() as temp_dir:
            cls(inputs=cls.Inputs.from_args(), temp_dir_path=Path(temp_dir)).__main()

    def __main(self):
        # https://docs.google.com/spreadsheets/d/1j2oaMvMxY4pnXO-sEH_fky2R2gm6TQeIev_Q8rVOD4M/edit?usp=sharing
        extractor: Optional[Extractor] = None
        try:
            spreadsheet_url = urlparse(self.__inputs.spreadsheet)
            if spreadsheet_url.hostname == "docs.google.com":
                if spreadsheet_url.path.startswith("/spreadsheets/d/"):
                    extractor = GoogleSheetsExtractor(
                        extracted_data_dir_path=self._extracted_data_dir_path,
                        spreadsheet_id=spreadsheet_url.path[
                            len("/spreadsheets/d/") :
                        ].split("/", 1)[0],
                    )
        except ValueError:
            pass

        spreadsheet_file_path = Path(self.__inputs.spreadsheet)
        if spreadsheet_file_path.is_file():
            if str(spreadsheet_file_path).lower().endswith(".xlsx"):
                extractor = Excel2010Extractor(xlsx_file_path=spreadsheet_file_path)

        if extractor is None:
            extractor = GoogleSheetsExtractor(
                extracted_data_dir_path=self._extracted_data_dir_path,
                spreadsheet_id=self.__inputs.spreadsheet,
            )

        Pipeline(
            extractor=extractor,
            id=self.__inputs.pipeline_id,
            loader=self._create_loader(),
            transformer=SpreadsheetTransformer(
                pipeline_id=self.__inputs.pipeline_id,
                root_model_classes_by_name=ROOT_MODEL_CLASSES_BY_NAME,
            ),
        ).extract_transform_load()


if __name__ == "__main__":
    Action.main()
