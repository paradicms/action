#!/usr/bin/env python3
import dataclasses
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from paradicms_etl.extractor import Extractor
from paradicms_etl.extractors.excel_2010_extractor import Excel2010Extractor
from paradicms_etl.extractors.google_sheets_extractor import GoogleSheetsExtractor
from paradicms_etl.pipeline import Pipeline
from paradicms_etl.transformers.spreadsheet_transformer import SpreadsheetTransformer
from paradicms_ssg.github_action import GitHubAction
from paradicms_ssg.github_action_inputs import GitHubActionInputs
from paradicms_ssg.models.root_model_classes_by_name import ROOT_MODEL_CLASSES_BY_NAME


@dataclass(frozen=True)
class _Inputs(GitHubActionInputs):
    spreadsheet: str = dataclasses.field(
        default=GitHubActionInputs.REQUIRED,
        metadata={
            "description": "Google Sheets spreadsheet id, Google Sheet URL, or path to an Excel 2010 (.xlsx) file"
        },
    )


class Action(GitHubAction[_Inputs]):
    """
    Generate a static site from a Paradicms-formatted spreadsheet.
    """

    @classmethod
    @property
    def _inputs_class(cls):
        return _Inputs

    def _run(self):
        extractor: Optional[Extractor] = None
        try:
            spreadsheet_url = urlparse(self._inputs.spreadsheet)
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

        spreadsheet_file_path = Path(self._inputs.spreadsheet)
        if spreadsheet_file_path.is_file():
            if str(spreadsheet_file_path).lower().endswith(".xlsx"):
                extractor = Excel2010Extractor(xlsx_file_path=spreadsheet_file_path)

        if extractor is None:
            extractor = GoogleSheetsExtractor(
                extracted_data_dir_path=self._extracted_data_dir_path,
                spreadsheet_id=self._inputs.spreadsheet,
            )

        Pipeline(
            extractor=extractor,
            id=self._inputs.pipeline_id,
            loader=self._create_loader(),
            transformer=SpreadsheetTransformer(
                pipeline_id=self._inputs.pipeline_id,
                root_model_classes_by_name=ROOT_MODEL_CLASSES_BY_NAME,
            ),
        ).extract_transform_load(force_extract=self._force_extract)


if __name__ == "__main__":
    Action.main()
