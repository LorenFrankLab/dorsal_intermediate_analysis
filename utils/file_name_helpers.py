import itertools
import os
import pandas as pd
import re

class FileNameHelper():

    from .user_settings import (data_path, epoch_names_list, camera_names_list)
    _data_path = data_path
    _epoch_names_list = epoch_names_list
    _camera_names_list = camera_names_list

    def __init__(self, subject_name, dates=None, n_epochs=None):
        
        # Ensure number of dates matches number of epoch counts
        if all([dates, n_epochs]) and len(dates) != len(n_epochs):
            raise ValueError(f"Number of dates doesn't match number of epoch counts")
        self._subject_name = subject_name
        self._dates = dates
        self._n_dates = len(self._dates) if dates is not None else None
        self._n_epochs = n_epochs

    def _get_file_name_prefix(self, date, epoch_idx=None, epoch_number=None, epoch_name=None, camera_name=None):

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
            epoch_number, epoch_name, camera_name = FileNameHelper._get_epoch_name_info(epoch_idx)
        
        # Get the file name prefix used for raw data if no epoch or camera are specified
        # By default, this is date + '_' + subject_name
        name_prefix = '_'.join([date, self._subject_name])

        # Get the file name prefix used for raw data if an epoch but no camera are specified
        # By default, this is date + '_' + subject_name + '_' epoch_number + '_' + epoch_name
        if all([epoch_number, epoch_name]):
            name_prefix = name_prefix + FileNameHelper._get_epoch_infix(epoch_number, epoch_name)
        
        # Get the file name prefix used for raw data if both an epoch and camera are specified
        # By default, this is date + '_' + subject_name + '_' epoch_number + '_' + epoch_name + '.' + camera_name
        if all([epoch_number, epoch_name, camera_name]):
            name_prefix = name_prefix + FileNameHelper._get_camera_infix(camera_name)       
        
        return name_prefix

    @classmethod
    def _get_epoch_name_info(cls, epoch_idx):

        # Ensure epoch number is a positive integer
        if type(epoch_idx) != int or epoch_idx <= 0:
            raise ValueError(f"Epoch number must be a positive integer")
        # Determine which epoch type the .rec file should correspond to
        epoch_type_idx = epoch_idx % len(FileNameHelper._epoch_names_list)
        epoch_type = str(FileNameHelper._epoch_names_list[epoch_type_idx])
        camera_name = str(FileNameHelper._camera_names_list[epoch_type_idx])
        # Determine how many times all epoch types have been cycled through
        cycle_number = str(epoch_idx // len(FileNameHelper._epoch_names_list))
        # Create epoch number and name
        epoch_number = '0' + str(epoch_idx)
        epoch_name = epoch_type + str(cycle_number)

        return epoch_number, epoch_name, camera_name

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
    
    def _get_paths_by_file_type(self, file_type):

        # Get file paths matching the file type
        file_paths = sorted(self._get_file_paths_dataframe_by_type(file_type).path_name.tolist())
        return file_paths
    
    def _get_expected_names_by_path_type(self, path_type):

        # Get expected file names matching the path type
        expected_file_names_df = self._get_expected_file_names_dataframe_by_type(path_type)
        expected_file_names = sorted(expected_file_names_df.file_names.tolist())
        expected_full_names = sorted(expected_file_names_df.full_names.tolist())
        return expected_file_names, expected_full_names


    def _get_file_paths_dataframe_by_type(self, path_type):
        
        # Get the path of the specified type
        if path_type == 'sessions':
            path_names = self._get_sessions_path()
        elif path_type == 'raw':
            path_names = self._get_raw_path()
        elif path_type == 'metadata':
            path_names = self._get_metadata_path()
        elif path_type == 'yml':
            path_names = self._get_yml_files_path()
        elif path_type == 'nwb':
            path_names = self._get_nwb_files_path()
        elif path_type == 'video':
            path_names = self._get_video_files_path()
        else:
            raise NotImplementedError(f"Couldn't get path for file type '{path_type}'")

        # Transform list of path names to dataframe
        path_names = list(itertools.chain(path_names))
        path_names_df = pd.DataFrame(path_names, columns=['path_name'])
        path_names_df['path_type'] = path_type
        return path_names_df

    def _get_sessions_path(self):

        # Get the directory holding all subjects' data for each session
        # By default, this is data_path/subject_name/raw
        sessions_path = [os.path.join(FileNameHelper._data_path, self._subject_name, 'raw')]
        return sessions_path

    def _get_raw_path(self):

        # Get the directory holding a subject's .rec files for the given days of recording
        # By default, this is data_path/subject_name/raw/date
        sessions_path = self._get_sessions_path()[0]
        raw_path = [os.path.join(sessions_path, date) for date in self._dates]
        return raw_path

    def _get_metadata_path(self):

        # Get the directory holding a subject's metadata files
        # By default, this is data_path/subject_name/metadata
        metadata_path = [os.path.join(FileNameHelper._data_path, self._subject_name, 'metadata')]
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
        nwb_files_path = [os.path.join(FileNameHelper._data_path, self._subject_name, 'nwb', 'raw')]
        return nwb_files_path

    def _get_video_files_path(self):

        # Get the directory holding a subject's .h264 files
        # By default, this is data_path/subject_name/nwb/video
        video_files_path = [os.path.join(FileNameHelper._data_path, self._subject_name, 'nwb', 'video')]
        return video_files_path


    def _get_expected_file_names_dataframe_by_type(self, file_type):

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
        path_names, file_names, full_names = FileNameHelper._get_filtered_file_names_info(self._get_paths_by_file_type(file_type), file_names)
        file_names_data = zip( list(itertools.chain(*path_names)), list(itertools.chain(*file_names)), list(itertools.chain(*full_names)) )
        file_names_df = pd.DataFrame(file_names_data, columns=['path_name', 'file_name', 'full_name'])
        file_names_df['file_type'] = file_type
        return file_names_df

    def _get_expected_raw_file_names(self):

        # Get raw data file name extensions
        name_parts_dict = FileNameHelper._create_file_name_extensions()['raw']
        file_names = [None]*self._n_dates
        # Get expected file names for each date and epoch
        for ndx, (date, n_epochs) in enumerate(zip(self._dates, self._n_epochs)):
            file_names[ndx] = [None]*n_epochs
            for epoch_idx in range(1, n_epochs+1):
                # Get epoch and camera name info
                epoch_number, epoch_name, camera_name = FileNameHelper._get_epoch_name_info(epoch_idx)
                
                # Define file name parts for each association
                name_parts_dict['infixes'] = {'epoch' : FileNameHelper._get_epoch_infix(epoch_number, epoch_name),
                                              'camera' : FileNameHelper._get_camera_infix(camera_name)}
                # Default prefix for file names with no association
                name_parts_dict['default_prefix'] = self._get_file_name_prefix(date)
                file_names[ndx][epoch_idx-1] = FileNameHelper._create_expected_file_names(name_parts_dict)
            
            # Merge file names across epochs within a date
            file_names[ndx] = list(set( itertools.chain(*file_names[ndx]) ))
        return file_names

    def _get_expected_metadata_file_names(self):

        # Get metadata file name extensions
        name_parts_dict = FileNameHelper._create_file_name_extensions()['metadata']
        # Create file name infixes for files associated with a date
        name_parts_dict['prefixes'] = {'subject' : self._subject_name}
        # Default prefix for file names with no association
        file_names = [FileNameHelper._create_expected_file_names(name_parts_dict)]
        return file_names

    def _get_expected_yml_file_names(self):

        # Get yml file name extensions
        name_parts_dict = FileNameHelper._create_file_name_extensions()['yml']
        file_names = [None]*self._n_dates
        for ndx, date in enumerate(self._dates):
            # Default prefix for file names with no association
            name_parts_dict['default_prefix'] = self._get_file_name_prefix(date)
            file_names[ndx] = FileNameHelper._create_expected_file_names(name_parts_dict)
        return file_names

    def _get_expected_nwb_file_names(self):

        # Get nwb file name extensions
        name_parts_dict = FileNameHelper._create_file_name_extensions()['nwb']
        file_names = [None]*self._n_dates
        for ndx, date in enumerate(self._dates):
            # Default prefix for file names with no association
            name_parts_dict['default_prefix'] = ''.join([self._subject_name, '_', date])
            file_names[ndx] = FileNameHelper._create_expected_file_names(name_parts_dict)
        return file_names

    def _get_expected_video_file_names(self):
       
       # Get video file name extensions
        name_parts_dict = FileNameHelper._create_file_name_extensions()['video']
        file_names = [None]*self._n_dates
        # Get expected file names for each date and epoch
        for ndx, (date, n_epochs) in enumerate(zip(self._dates, self._n_epochs)):
            for epoch_idx in range(1, n_epochs+1):
                # Default prefix for file names with no association
                name_parts_dict['default_prefix'] = self._get_file_name_prefix(date, epoch_idx=epoch_idx)
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


    @staticmethod
    def _get_filtered_file_names_info(file_paths, file_names, match_pattern='.*'):

        # Filter file names using regular expressions and get file paths and full names
        path_names = [None]*len(file_names)
        matching_names = [None]*len(file_names)
        full_matching_names = [None]*len(file_names)

        for ndx, names in enumerate(file_names):
            # Restrict file names to those matching the regular expression pattern
            matching_names[ndx] = FileNameHelper._file_names_regex(names, match_pattern, full_match=False)
            # Get file path of matching file names
            if len(file_paths) == len(matching_names):
                path_idx = ndx
            else:
                path_idx = 0
            path_names[ndx] = [file_paths[path_idx]]*len(matching_names[ndx])
            # Get full names for matching file names
            full_matching_names[ndx] = FileNameHelper._file_names_to_full_names(matching_names[ndx], file_paths[path_idx])

        return path_names, matching_names, full_matching_names
    
    @staticmethod
    def _file_names_regex(file_names, pattern, full_match=False):
       
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
    def _compare_file_names(file_names, valid_names):

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
    def _file_names_to_full_names(file_names, file_path):

        # Convert list of file names to list of full names
        full_names = [os.path.join(file_path, name) for name in file_names]
        return full_names

    @staticmethod
    def _full_names_to_file_names(full_names):

        # Return empty lists if list of names is empty
        if not full_names:
            return [], []
        # Convert list of full names to lists of file names and file paths
        file_name_data = [os.path.split(name) for name in full_names]
        file_paths, file_names = zip(*file_name_data)
        return list(file_paths), list(file_names)