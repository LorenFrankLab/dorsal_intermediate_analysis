from itertools import chain
import os
import pandas as pd
import re

from utils import verbose_printer
from utils.abstract_classes import DataReader
from utils.data_helpers import parse_iterable_inputs, parse_iterable_outputs

class FileNameHelper(DataReader):

    from config import (epoch_names_list, camera_names_list)
    epoch_names_list = epoch_names_list
    camera_names_list = camera_names_list
    file_types = ('raw', 'metadata', 'yml', 'nwb', 'video')
    
    def __init__(self, subject_name, dates=None):
        
        super().__init__(subject_name, dates=dates)
        self._path_names = None
        self._expected_file_names = None

    @property
    def path_names(self):
        if self._path_names is None:
            self._update_path_names()
        return self._path_names
    
    @property
    def expected_file_names(self):
        if self._expected_file_names is None:
            self._update_expected_file_names()
        return self._expected_file_names
    

    @classmethod
    def get_epoch_name_info(cls, epoch_idx):

        epoch_idx = parse_iterable_inputs(epoch_idx)
        epoch_numbers = [None]*len(epoch_idx)
        epoch_names = [None]*len(epoch_idx)
        camera_names = [None]*len(epoch_idx)
        for ndx, idx in enumerate(epoch_idx):
            # Ensure epoch number is a positive integer
            if type(idx) != int or idx <= 0:
                raise ValueError(f"Epoch number must be a positive integer")
            # Determine which epoch type the .rec file should correspond to
            epoch_type_idx = idx % len(FileNameHelper.epoch_names_list)
            epoch_type = str(FileNameHelper.epoch_names_list[epoch_type_idx])
            camera_name = str(FileNameHelper.camera_names_list[epoch_type_idx])
            # Determine how many times all epoch types have been cycled through
            cycle_number = str(idx // len(FileNameHelper.epoch_names_list))
            # Create epoch number and name
            epoch_number = '0' + str(idx)
            epoch_name = epoch_type + str(cycle_number)

            epoch_numbers[ndx] = epoch_number
            epoch_names[ndx] = epoch_name
            camera_names[ndx] = camera_name
            
        data_dict = {'epoch_number' : epoch_numbers, 'epoch_name' : epoch_names, 'camera_name' : camera_names}
        epoch_info_df = pd.DataFrame.from_dict(data_dict)
        return epoch_info_df

    @staticmethod
    def get_file_name_info(path_names, file_names):

        path_names, file_names = parse_iterable_inputs(path_names, file_names)
        if len(path_names) != 1 and len(path_names) != len(file_names):
            raise ValueError(f"Length of 'path_names' must be the same as length of 'file_names'")
        # Initialize dataframe with file info
        file_names_df = pd.DataFrame(columns=['path_name', 'file_name', 'full_name'])
        # Use same path name for all files if only one path is given
        if len(path_names) == 1:
            path_names = [path_names[0]]*len(file_names)
        
        # Store the path name, file names, and full names for each group of file names
        names_df_list = [None]*len(file_names)
        for ndx, (path_name, names) in enumerate(zip(path_names, file_names)):
            n_files = len(names)
            data_dict = {'path_name' : [path_name]*n_files, 'file_name' : names}
            data_dict['full_name'] = FileNameHelper.convert_names_to_full_names(path_name, names)
            # Convert data dictionary to a dataframe and append it to the existing names dataframe
            data_df = pd.DataFrame.from_dict(data_dict)
            names_df_list[ndx] = data_df
        file_names_df = pd.concat(names_df_list)
        return file_names_df

    @staticmethod
    def filter_file_names(file_names, pattern, full_match=False):
       
        file_names = parse_iterable_inputs(file_names)
        # Return empty list if list of file names is empty
        if not file_names:
            return []
        # Get file names matching the specified pattern
        if full_match:
            matching_names = [name for name in file_names if re.fullmatch(pattern, name)]
        else:
            matching_names = [name for name in file_names if re.search(pattern, name)]
        return matching_names
    
    @staticmethod
    def filter_dir_file_names(path_name, pattern, full_match=False):

        # Find all files in specified file path matching the regular expression pattern
        file_names = [file.name for file in os.scandir(path_name) if file.is_file()]
        matching_names = FileNameHelper.filter_file_names(file_names, pattern, full_match=full_match)
        return matching_names

    @staticmethod
    def compare_file_names(file_names, valid_names):

        file_names, valid_names = parse_iterable_inputs(file_names, valid_names)
        # Ensure that lists of file names and valid names are unique
        assert len(valid_names) == len(set(valid_names)), \
            f"Repeat names found in list of expected file names"
        assert len(file_names) == len(set(file_names)), \
            f"Repeat names found in list of file names"
        
        # Get file names that aren't valid names
        file_names_diff = set(valid_names) - set(file_names)
        # Get file names that are valid names
        file_names_match = set(valid_names) & set(file_names)
        return list(file_names_match), list(file_names_diff)

    @staticmethod
    def convert_names_to_full_names(path_name, file_names):

        file_names = parse_iterable_inputs(file_names)
        # Convert list of file names to list of full names
        full_names = parse_iterable_outputs( [os.path.join(path_name, name) for name in file_names] )
        return full_names

    @staticmethod
    def convert_full_names_to_names(full_names):

        full_names = parse_iterable_inputs(full_names)
        # Return empty lists if list of names is empty
        if not full_names:
            return [], []
        # Convert list of full names to lists of file names and file paths
        file_name_data = [os.path.split(name) for name in full_names]
        path_names, file_names = zip(*file_name_data)
        path_names, file_names = parse_iterable_outputs(list(path_names), list(file_names))
        return path_names, file_names

    def get_file_name_prefix(self, date, epoch_idx=None, epoch_number=None, epoch_name=None, camera_name=None):

        # Ensure that either epoch index or epoch information are specified but not both
        if epoch_idx and any([epoch_number, epoch_name, camera_name]):
            raise ValueError(f"Both an epoch index and epoch information were specified")
        # Ensure both epoch number and name are specified together
        if (epoch_number and not epoch_name) or (not epoch_number and epoch_name):
            raise ValueError(f"Both an epoch number and an epoch name must be specified")
        # Ensure epoch number and name are specified together with a camera name
        if camera_name and not all([epoch_number, epoch_name]):
            raise ValueError(f"An epoch number and name must be specified when a camera name is specified")
        # Get epoch number and name and camera name from epoch index
        if epoch_idx:
            epoch_info = FileNameHelper.get_epoch_name_info(epoch_idx).iloc[0]
            epoch_number, epoch_name, camera_name = epoch_info.epoch_number, epoch_info.epoch_name, epoch_info.camera_name
        
        # Get the file name prefix used for raw data if no epoch or camera are specified
        # By default, this is date + '_' + subject_name
        name_prefix = '_'.join([date, self.subject_name])

        # Get the file name prefix used for raw data if an epoch but no camera are specified
        # By default, this is date + '_' + subject_name + '_' epoch_number + '_' + epoch_name
        if all([epoch_number, epoch_name]):
            name_prefix = name_prefix + FileNameHelper._get_epoch_infix(epoch_number, epoch_name)
        
        # Get the file name prefix used for raw data if both an epoch and camera are specified
        # By default, this is date + '_' + subject_name + '_' epoch_number + '_' + epoch_name + '.' + camera_name
        if all([epoch_number, epoch_name, camera_name]):
            name_prefix = name_prefix + FileNameHelper._get_camera_infix(camera_name)       
        
        return name_prefix

    @staticmethod
    def _get_epoch_infix(epoch_number, epoch_name):

        # Get the file name infix used for a given epoch of recording
        # By default, this is '_' + epoch_nmber + '_' + epoch_name
        epoch_infix = ''.join(['_', epoch_number, '_', epoch_name])
        return epoch_infix

    @staticmethod
    def _get_camera_infix(camera_name):

        # Get the file name infix used for raw data associated with a video camera
        # By default, this is '.' + camera_name
        camera_infix = '.' + camera_name
        return camera_infix


    def _get_path_names_by_type(self, file_type):
        
        # Get the path of the specified type
        if file_type == 'raw':
            path_names = self._get_raw_path()
        elif file_type == 'metadata':
            path_names = self._get_metadata_path()
        elif file_type == 'yml':
            path_names = self._get_yml_files_path()
        elif file_type == 'nwb':
            path_names = self._get_nwb_files_path()
        elif file_type == 'video':
            path_names = self._get_video_files_path()
        else:
            raise NotImplementedError(f"Couldn't get path for file type '{file_type}'")

        # Transform list of path names to dataframe
        path_names = list(chain(path_names))
        path_names_df = pd.DataFrame(path_names, columns=['path_name'])
        path_names_df['file_type'] = file_type
        return path_names_df

    def _get_raw_path(self):

        # Get the directory holding a subject's .rec files for the given days of recording
        # By default, this is data_path/subject_name/raw/date
        raw_path = [os.path.join(self._sessions_path, date) for date in self.dates]
        return raw_path

    def _get_metadata_path(self):

        # Get the directory holding a subject's metadata files
        # By default, this is data_path/subject_name/metadata
        metadata_path = [os.path.join(FileNameHelper.data_path, self.subject_name, 'metadata')]
        return metadata_path

    def _get_yml_files_path(self):

        # Get the directory holding a subject's .yml files for each recording session
        # By default, this is data_path/subject_name/metadata/yml
        metadata_path = self._get_metadata_path()[0]
        yml_files_path = [os.path.join(metadata_path, 'yml')]
        return yml_files_path

    def _get_nwb_files_path(self):

        # Get the directory holding a subject's raw .nwb files
        # By default, this is data_path/subject_name/nwb/raw
        nwb_files_path = [os.path.join(FileNameHelper.data_path, self.subject_name, 'nwb', 'raw')]
        return nwb_files_path

    def _get_video_files_path(self):

        # Get the directory holding a subject's .h264 files
        # By default, this is data_path/subject_name/nwb/video
        video_files_path = [os.path.join(FileNameHelper.data_path, self.subject_name, 'nwb', 'video')]
        return video_files_path


    def _get_expected_file_names_by_type(self, file_type):

        # Get list of file names of the specified type
        if file_type == 'raw':
            file_names = self._get_expected_raw_file_names()
        elif file_type == 'metadata':
            file_names = self._get_expected_metadata_file_names()
        elif file_type == 'yml':
            file_names = self._get_expected_yml_file_names()
        elif file_type == 'nwb':
            file_names = self._get_expected_nwb_file_names()
        elif file_type == 'video':
            file_names = self._get_expected_video_file_names()
        else:
            raise NotImplementedError(f"Couldn't get expected file names for file type '{file_type}'")
        
        # Get full names for all file names and transform into a dataframe
        path_names = self._get_path_names_by_type(file_type)['path_name'].tolist()
        if file_type != 'raw':
            path_names = [path_names[0]]*len(file_names)
        file_names_df = FileNameHelper.get_file_name_info(path_names, file_names)
        file_names_df['file_type'] = file_type
        return file_names_df

    def _get_expected_raw_file_names(self):

        # Get raw data file name extensions
        name_parts_dict = FileNameHelper._create_file_name_extensions()['raw']
        file_names = [None]*self._n_dates
        # Get expected file names for each date and epoch
        for ndx, (date, n_epochs) in enumerate(zip(self.dates, self._n_epochs)):
            file_names[ndx] = [None]*n_epochs
            for epoch_idx in range(1, n_epochs+1):
                # Get epoch and camera name info
                epoch_info = FileNameHelper.get_epoch_name_info(epoch_idx)
                epoch_number, epoch_name, camera_name = epoch_info.iloc[0].values
                
                # Define file name parts for each association
                name_parts_dict['infixes'] = {'epoch' : FileNameHelper._get_epoch_infix(epoch_number, epoch_name),
                                              'camera' : FileNameHelper._get_camera_infix(camera_name)}
                # Default prefix for file names with no association
                name_parts_dict['default_prefix'] = self.get_file_name_prefix(date)
                file_names[ndx][epoch_idx-1] = FileNameHelper._create_expected_file_names(name_parts_dict)
            
            # Merge file names across epochs within a date
            file_names[ndx] = list(set( chain(*file_names[ndx]) ))
        return file_names

    def _get_expected_metadata_file_names(self):

        # Get metadata file name extensions
        name_parts_dict = FileNameHelper._create_file_name_extensions()['metadata']
        # Create file name infixes for files associated with a date
        name_parts_dict['prefixes'] = {'subject' : self.subject_name}
        # Default prefix for file names with no association
        file_names = [FileNameHelper._create_expected_file_names(name_parts_dict)]
        return file_names

    def _get_expected_yml_file_names(self):

        # Get yml file name extensions
        name_parts_dict = FileNameHelper._create_file_name_extensions()['yml']
        file_names = [None]*self._n_dates
        for ndx, date in enumerate(self.dates):
            # Default prefix for file names with no association
            name_parts_dict['default_prefix'] = self.get_file_name_prefix(date)
            file_names[ndx] = FileNameHelper._create_expected_file_names(name_parts_dict)
        return file_names

    def _get_expected_nwb_file_names(self):

        # Get nwb file name extensions
        name_parts_dict = FileNameHelper._create_file_name_extensions()['nwb']
        file_names = [None]*self._n_dates
        for ndx, date in enumerate(self.dates):
            # Default prefix for file names with no association
            name_parts_dict['default_prefix'] = ''.join([self.subject_name, '_', date])
            file_names[ndx] = FileNameHelper._create_expected_file_names(name_parts_dict)
        return file_names

    def _get_expected_video_file_names(self):
       
       # Get video file name extensions
        name_parts_dict = FileNameHelper._create_file_name_extensions()['video']
        file_names = [None]*self._n_dates
        # Get expected file names for each date and epoch
        for ndx, (date, n_epochs) in enumerate(zip(self.dates, self._n_epochs)):
            for epoch_idx in range(1, n_epochs+1):
                # Default prefix for file names with no association
                name_parts_dict['default_prefix'] = self.get_file_name_prefix(date, epoch_idx=epoch_idx)
                file_names[ndx] = FileNameHelper._create_expected_file_names(name_parts_dict)
        return file_names

    @classmethod    
    def _create_file_name_extensions(cls):

        # Create dictionary of file name extensions and which are associated with subjects, epochs, and cameras
        name_parts_dict = {'raw' : {'extensions' : ['.trodesconf',
                                                    '.rec',
                                                    '.stateScriptLog',
                                                    '.h264',
                                                    '.videoPositionTracking',
                                                    '.videoTimeStamps.cameraHWSync'],
                                    'associations' : {'epoch' : ['.rec',
                                                                 '.h264',
                                                                 '.stateScriptLog',
                                                                 '.videoPositionTracking',
                                                                 '.videoTimeStamps.cameraHWSync'],
                                                      'camera' : ['.h264',
                                                                  '.videoPositionTracking',
                                                                  '.videoTimeStamps.cameraHWSync']}},
                           'metadata' : {'extensions' : ['tetrode_12.5.yml',
                                                         '_cannula_diagram.svg',
                                                         '_electrode_arrangement.svg',
                                                         '_dio_events.csv',
                                                         '_electrode_info.csv',
                                                         '_session_info.csv',
                                                         '_subject_info.csv'],
                                         'associations' : {'subject' : ['_cannula_diagram.svg',
                                                                        '_electrode_arrangement.svg',
                                                                        '_dio_events.csv',
                                                                        '_electrode_info.csv',
                                                                        '_session_info.csv',
                                                                        '_subject_info.csv']}},
                           'yml' : {'extensions' : ['.yml'],
                                    'associations' : None},
                           'nwb' : {'extensions' : ['.nwb'],
                                    'associations' : None},
                           'video' : {'extensions' : ['.h264'],
                                      'associations' : None}}
        return name_parts_dict

    @staticmethod
    def _create_expected_file_names(name_parts_dict):

        if name_parts_dict is None:
            name_parts_dict = {'extensions' : [],
                               'associations' : [],
                               'prefixes' : {},
                               'infixes' : {},
                               'suffixes' : {},
                               'default_prefix' : ''}
        # Create expected file names for each file type
        file_names = set()
        for file_type in name_parts_dict['extensions']:
            # Get file name parts for associated file types
            associations = name_parts_dict['associations'].values() if name_parts_dict['associations'] is not None else []
            prefixes = name_parts_dict['prefixes'].values() if 'prefixes' in name_parts_dict.keys() else []
            infixes = name_parts_dict['infixes'].values() if 'infixes' in name_parts_dict.keys() else []
            suffixes = name_parts_dict['suffixes'].values() if 'suffixes' in name_parts_dict.keys() else []
            default_prefix = name_parts_dict['default_prefix'] if 'default_prefix' in name_parts_dict.keys() else ''
            
            # Determine which prefixes, infixes, and suffixes to combine
            prefix_list = [prefix for (prefix, match_types) in zip(prefixes, associations) if file_type in match_types]
            infix_list = [infix for (infix, match_types) in zip(infixes, associations) if file_type in match_types]
            suffix_list = [suffix for (suffix, match_types) in zip(suffixes, associations) if file_type in match_types]
            # Create file name
            file_names.add( default_prefix + ''.join(prefix_list + infix_list + suffix_list) + file_type )
        return list(file_names)
    

    def _update(self):
        
        # Get file paths and expected file names
        self._update_path_names()
        self._update_expected_file_names()

    def _update_path_names(self):

        # Get paths of all data files
        path_names_list = [None]*len(FileNameHelper.file_types)
        for ndx, file_type in enumerate(FileNameHelper.file_types):
            path_names_list[ndx] = self._get_path_names_by_type(file_type)
        self._path_names = pd.concat(path_names_list)
        self._path_names.reset_index(inplace=True, drop=True)

    def _update_expected_file_names(self):
    
        # Get expected file names for all data files
        expected_file_names_list = [None]*len(FileNameHelper.file_types)
        for ndx, file_type in enumerate(FileNameHelper.file_types):
            expected_file_names_list[ndx] = self._get_expected_file_names_by_type(file_type)
        self._expected_file_names = pd.concat(expected_file_names_list)
        self._expected_file_names.reset_index(inplace=True, drop=True)


    def _print(self):
        
        # Print file paths and expected file names associated with subject
        verbose_printer.print_header(f"{self.subject_name} data paths and expected file names")
        for file_type in FileNameHelper.file_types:
            path_names = self._path_names[self._path_names.file_type == file_type].path_name.tolist()
            
            if file_type == 'raw':
                for date, path_name in zip(self.dates, path_names):
                    verbose_printer.print_bold(f"{path_name}")
                    expected_file_names = self._expected_file_names[self._expected_file_names.file_type == file_type].file_name.tolist()
                    expected_file_names = FileNameHelper.filter_file_names(expected_file_names, date)
                    verbose_printer.print_iterable(expected_file_names, prefix='  ')
                    verbose_printer.print_newline()
            else:
                verbose_printer.print_bold(f"{path_names[0]}")
                expected_file_names = self._expected_file_names[self._expected_file_names.file_type == file_type].file_name.tolist()
                verbose_printer.print_iterable(expected_file_names, prefix='  ')
                verbose_printer.print_newline()