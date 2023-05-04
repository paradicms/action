#!/usr/bin/env python3
import dataclasses
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from paradicms_etl.etl_github_action import EtlGitHubAction
from paradicms_etl.extractor import Extractor
from paradicms_etl.extractors.excel_2010_extractor import Excel2010Extractor
from paradicms_etl.extractors.google_sheets_extractor import GoogleSheetsExtractor
from paradicms_etl.transformers.spreadsheet_transformer import SpreadsheetTransformer
from paradicms_ssg.models.root_model_classes_by_name import ROOT_MODEL_CLASSES_BY_NAME


class Action(EtlGitHubAction):
    """
    Extract, transform, and load data from a Paradicms-formatted spreadsheet.
    """

    @dataclass(frozen=True)
    class Inputs(EtlGitHubAction.Inputs):
        spreadsheet: str = dataclasses.field(
            default=EtlGitHubAction.Inputs.REQUIRED,
            metadata={
                "description": "Google Sheets spreadsheet id, Google Sheet URL, or path to an Excel 2010 (.xlsx) file"
            },
        )

    def __init__(self, *, spreadsheet: str, **kwds):
        EtlGitHubAction.__init__(self, **kwds)
        self.__spreadsheet = spreadsheet

    def _run(self):
        extractor: Optional[Extractor] = None
        try:
            spreadsheet_url = urlparse(self.__spreadsheet)
            if spreadsheet_url.hostname == "docs.google.com":
                if spreadsheet_url.path.startswith("/spreadsheets/d/"):
                    extractor = GoogleSheetsExtractor(
                        cache_dir_path=self._cache_dir_path / "google-sheets",
                        spreadsheet_id=spreadsheet_url.path[
                            len("/spreadsheets/d/") :
                        ].split("/", 1)[0],
                    )
        except ValueError:
            pass

        spreadsheet_file_path = Path(self.__spreadsheet)
        if spreadsheet_file_path.is_file():
            if str(spreadsheet_file_path).lower().endswith(".xlsx"):
                extractor = Excel2010Extractor(xlsx_file_path=spreadsheet_file_path)

        if extractor is None:
            extractor = GoogleSheetsExtractor(
                cache_dir_path=self._cache_dir_path / "google-sheets",
                spreadsheet_id=self.__spreadsheet,
            )

        self._run_pipeline(
            extractor=extractor,
            transformer=SpreadsheetTransformer(
                pipeline_id=self._pipeline_id,
                root_model_classes_by_name=ROOT_MODEL_CLASSES_BY_NAME,
            ),
        )


if __name__ == "__main__":
    Action.main()
