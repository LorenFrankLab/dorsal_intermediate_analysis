import warnings
warnings.filterwarnings('ignore')

from raw_conversion.file_info import FileInfo

from sg_common_utils.preprocessing_info import PreprocessingInfo
from sg_common_utils.sg_helpers import sql_or_query
from sg_common_utils.sg_tables import Nwbfile

from spyglass.data_import import insert_sessions

from utils import verbose_printer
from utils.abstract_classes import DataWriter
from utils.data_containers import parse_iterable_inputs
from utils.file_helpers import no_overwrite_handler_table
from utils.function_wrappers import function_timer


class InsertionProcessor(DataWriter):

    def __init__(self, subject_name, dates=None, overwrite=False, verbose=False, timing=False):

        # Validate subject name and dates for insertion
        super().__init__(subject_name, dates=dates, overwrite=overwrite, verbose=verbose, timing=timing)

        # Get file name info
        self._file_info = FileInfo(subject_name=self.subject_name, dates=self.dates)
        # Get preprocessing analyses info
        self._preprocessing_info = PreprocessingInfo(subject_names=self.subject_name)
        self._inserted_nwb_file_names = self._preprocessing_info.nwb_file_names
    
    @property
    def inserted_nwb_file_names(self):
        if self._inserted_nwb_file_names is None:
            self._update()
        return self._inserted_nwb_file_names
        
    @function_timer
    def insert_nwb_files(self):

        self._update()
        # Insert each .nwb file into the database
        for date in self.dates:
            self._insert_nwb_file_into_tables(date)

    @function_timer
    def _insert_nwb_file_into_tables(self, date):

        # Ensure .nwb file isn't already inserted in the tables
        file_exists_bool, nwb_file_name = self._validate_nwb_file_insertion(date)
        # Insert each .nwb file into the database        
        if file_exists_bool:
            if not self._overwrite:
                no_overwrite_handler_table(f"Entry '{nwb_file_name}' already exists in 'Nwbfile' table")
            else:
                InsertionProcessor._depopulate_nwb_file_table(nwb_file_name)
                InsertionProcessor._populate_nwb_file_table(nwb_file_name)
        else:
            InsertionProcessor._populate_nwb_file_table(nwb_file_name)
    
    @function_timer
    def remove_nwb_files(self):

        # Delete each .nwb file from the database
        for nwb_file_name in self._preprocessing_info.nwb_file_names:
            verbose_printer.print_header(f"{nwb_file_name}")
            InsertionProcessor._depopulate_nwb_file_table(nwb_file_name)
        
    def _validate_nwb_file_insertion(self, date):

        # Get name of .nwb file that will be inserted into the tables
        nwb_file_name = self._file_info.search_expected_file_names([date, '.nwb']).iloc[0].file_name
        nwb_file_name = nwb_file_name.split('.nwb')[0] + '_.nwb'
        # Check if .nwb file has already been inserted into tables
        file_exists_bool = nwb_file_name in self.inserted_nwb_file_names
        return file_exists_bool, nwb_file_name
        

    @staticmethod
    def _depopulate_nwb_file_table(nwb_file_names):
        
        nwb_file_names = parse_iterable_inputs(nwb_file_names)
        # Delete .nwb file from Nwbfile table and remove ephys .nwb file from Spyglass directory
        (Nwbfile & sql_or_query('nwb_file_name', nwb_file_names)).delete()
        Nwbfile().cleanup(delete_files=True)

    @staticmethod
    def _populate_nwb_file_table(nwb_file_name):
        
        # Add .nwb file to Nwbfile table
        nwb_file_name = nwb_file_name.split('_.nwb')[0] + '.nwb'
        insert_sessions(nwb_file_name)
    
    def _update(self):

        # Get list of already inserted files
        self._preprocessing_info = PreprocessingInfo(subject_names=self.subject_name)
        self._inserted_nwb_file_names = self._preprocessing_info.nwb_file_names
    
    def _print(self, dates):

        for date in dates:
            # Print .yml file existence and permissions for the given date
            verbose_printer.print_header(f"{self.subject_name} {date}")
            file_exists_bool, nwb_file_name = self._validate_nwb_file_insertion(date)
            if file_exists_bool:
                verbose_printer.print_pass(f"Found inserted .nwb file {nwb_file_name}")
            else:
                verbose_printer.print_fail(f"Couldn't find inserted .nwb file {nwb_file_name}")
            verbose_printer.print_newline()