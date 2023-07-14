from collections import namedtuple
import numpy as np
import os
import pandas as pd
import re

from .common_abstract_classes import DataWriter
from .common_helpers import (print_verbose, function_timer, no_overwrite_handler_file)
from .file_utils import (FileInfo, FileNameHelper)

class MetadataWriter(DataWriter):

    # Tuple of valid metadata types
    _metadata_types = ('subject', 'session', 'electrode', 'dio')

    # Initialize named tuples for devices, tasks, behavioral events, and electrode groups
    _Device = namedtuple("Device", "name data")
    _Camera = namedtuple("Camera", "id data")
    _Statescript = namedtuple("Statescript", "name data")
    _Video = namedtuple("Video", "name data")
    _Task = namedtuple("Task", "task_name data")
    _Event = namedtuple("Event", "description data")
    _Electrode = namedtuple("Electrode", "id data")
    _Map = namedtuple("Map", "ntrode_id data")
    # Dictionary of initialization methods for the named tuples
    _named_tuple_dict = {'device' : _Device,
                         'camera' : _Camera,
                         'statescript' : _Statescript,
                         'video' : _Video,
                         'task' : _Task,
                         'event' : _Event,
                         'electrode' : _Electrode,
                         'map' : _Map}

    def __init__(self, subject_name, dates=None, overwrite=False, verbose=False, timing=False):

        # Enable overwriting existing .yml files, verbose output, and timing
        super().__init__(overwrite=overwrite, verbose=verbose, timing=timing)

        # Get file information for raw data and metadata
        self._file_info = FileInfo(subject_name, dates=dates)       
        # Get subject and dates information
        self._subject_name = self._file_info._subject_name
        self._dates = self._file_info._dates
        
        # Get index column name and labels and column names used to slice metadata
        self._metadata_slice_names = MetadataWriter._create_metadata_slice_names()
        # Get only metadata for the specified subject and dates
        self._metadata_slice_names['subject']['labels'] = subject_name
        self._metadata_slice_names['session']['labels'] = dates
        
        # Read .csv metadata files into dataframes and convert them to nested dictionaries
        self._update()


    @property
    def subject_name(self):
        return self._subject_name

    @property
    def dates(self):
        return self._dates

    @property
    def metadata(self):
        return self._metadata
        

    @function_timer
    def create_yml_files(self):

        # Create a .yml metadata file for each date
        for date in self._dates:
            self._write_metadata_file(date)
    
    def print(self, dates=None):

        # Print metadata for all dates if no dates are specified
        if dates is None:
            dates = self._dates
        else:
            # Ensure specified dates are valid
            dates_diff = set(dates) - set(self._dates)
            if dates_diff:
                raise ValueError(f"The following dates weren't found in the MetadataWriter instance: {dates_diff}")
        
        # Print metadata
        for date in dates:
            txt = self._metadata_to_text(date)
            print(f"{txt}\n")

    def _update(self):
        
        # Update file names, expected file names, and missing and matching file names
        self._file_info._update()
        # Read .csv metadata into dataframes
        self._csv_file_names = self._get_csv_file_names()
        self._csv_metadata = self._get_csv_metadata()
        # Parse metadata to nested dictionaries that will be converted to .yml files
        self._metadata = self._create_yml_metadata()


    def _get_csv_file_names(self):

        # Get full names of .csv metadata files from FileInfo dataframes
        suffix_dict = MetadataWriter._create_csv_file_suffixes()
        csv_name_dict = {key: None for key in suffix_dict.keys()}
        for metadata_type, suffix in suffix_dict.items():
            name = self._file_info.search_matching_file_names(suffix).full_name.tolist()
            # Ensure that exactly 1 .csv metadata is found for the specified metadata type
            assert name, \
                f"No .csv metadata file found for metadata type '{metadata_type}'"
            assert len(name) == 1, \
                f"More than 1 .csv metadata file found for metadata type '{metadata_type}'"
            csv_name_dict[metadata_type] = name[0]
        return csv_name_dict
        
    def _get_csv_metadata(self):

        # Convert .csv file into dataframe
        metadata_dict = {key : None for key in self._csv_file_names.keys()}
        for metadata_type, name in self._csv_file_names.items():
            metadata_df = pd.read_csv(name)
            # Set index column and convert labels to strings
            metadata_df.set_index(self._metadata_slice_names[metadata_type]['index_name'], inplace=True)
            metadata_df.index = metadata_df.index.map(str)
            
            # Slice dataframe by labels
            labels = self._metadata_slice_names[metadata_type]['labels']
            if labels:
                metadata_df = metadata_df.loc[labels]
            # Keep only a subset of dataframe columns
            column_names = self._metadata_slice_names[metadata_type]['column_names']
            if column_names:
                metadata_df = metadata_df[column_names]
            
            metadata_dict[metadata_type] = metadata_df
        return metadata_dict


    def _create_yml_metadata(self):

        metadata = {}
        # Get metadata for each date
        for date in self._dates:
            metadata[date] = self._parse_metadata(date)
        return metadata
    
    def _parse_metadata(self, date):

        subject_df = self._csv_metadata['subject']
        session_df = self._csv_metadata['session'].loc[date]
        electrode_df = self._csv_metadata['electrode']
        dio_df = self._csv_metadata['dio']
        # Get metadata template
        template_dict = MetadataWriter._get_yml_template()
        
        # Determine session ID, number of epochs in recording, and task type
        session_id = self._file_info.get_session_dates().index(date)+1
        n_epochs = int(session_df['Number of epochs'])
        session_description = session_df['Session description']
        # Determine task type and description from session description
        if re.match('Exploration', session_description):
            task_name = 'Exploration and rest'
            task_description = 'Exploration with rest periods in home box'
        elif re.match('Alternation', session_description):
            task_name = 'Alternation and rest'
            task_description = 'Alternation with rest periods in home box'
        else:
            raise ValueError(f"Unable to determine task type from session description '{session_description}' on '{date}_{self._subject_name}'")

        # Generate epoch-specific metadata       
        task_dict = {}
        task_dict['epoch_numbers_list'] = list(range(1, n_epochs+1))
        # Get sorted statescript file names and full paths
        task_dict['statescript_file_names_list'] = sorted(self._file_info.search_matching_file_names('^' + date + '.*.stateScriptLog$').full_name.tolist())
        task_dict['statescript_names_list'] = ['statescript_' + FileNameHelper._get_epoch_name_info(ndx+1)[1] for ndx in range(self._file_info.get_number_of_epochs(dates=[date])[0])]
        # Get video file and camera names
        task_dict['video_file_names_list'] = sorted(self._file_info.search_matching_file_names('^' + date + '.*.h264$').file_name.tolist())
        task_dict['camera_names_list'] = [FileNameHelper._get_epoch_name_info(epoch)[2] for epoch in range(1, n_epochs+1)]

        # Fill .yml file template with subject and session metadata
        template_dict['experimenter_name'] = session_df['Experimenters'].split(', ')
        template_dict['experiment_description'] = 'Four-box, six-arm automated track'
        template_dict['session_description'] = session_description
        template_dict['session_id'] = session_id
        template_dict['subject']['genotype'] = subject_df['Genotype']
        template_dict['subject']['sex'] = subject_df['Sex']
        template_dict['subject']['subject_id'] = self._subject_name

        # Fill .yml file template with acquisition metadata
        device_tuple = MetadataWriter._create_metadata_named_tuple('trodes', 'device')
        template_dict['data_acq_device'].append(device_tuple)
        
        # Fill .yml file template with camera metadata
        for ndx, camera_name in enumerate(set(task_dict['camera_names_list'])):
            camera_data = {'meters_per_pixel' : float(subject_df['Video scale'].split(' ')[0]),
                           'camera_name' : camera_name}
            camera_tuple = MetadataWriter._create_metadata_named_tuple(ndx, 'camera', params=camera_data)
            template_dict['cameras'].append(camera_tuple)
        
        # Fill .yml file template with statescript log and video file metadata
        for ndx in range(n_epochs):
            statescript_data = {'path' : task_dict['statescript_file_names_list'][ndx],
                                'task_epochs' : [ndx+1]}
            statescript_tuple = MetadataWriter._create_metadata_named_tuple(task_dict['statescript_names_list'][ndx], 'statescript', params=statescript_data)
            template_dict['associated_files'].append(statescript_tuple)
            
            camera_name = task_dict['camera_names_list'][ndx]
            video_data = {'camera_id' : list(set(task_dict['camera_names_list'])).index(camera_name),
                          'task_epoch' : task_dict['epoch_numbers_list'][ndx]}
            video_tuple = MetadataWriter._create_metadata_named_tuple(task_dict['video_file_names_list'][ndx], 'video', params=video_data)
            template_dict['associated_video_files'].append(video_tuple)
        
        # Fill .yml file template with task metadata
        camera_names_set = set(task_dict['camera_names_list'])
        camera_ids_list = [task_dict['camera_names_list'].index(name) for name in camera_names_set]
        task_data = {'task_description' : task_description,
                     'camera_id' : camera_ids_list,
                     'task_epochs' : task_dict['epoch_numbers_list']}
        task_tuple = MetadataWriter._create_metadata_named_tuple(task_name, 'task', params=task_data)
        template_dict['tasks'].append(task_tuple)

        # Fill .yml file with behavioral events (DIO) metadata
        for ndx, dio_name in enumerate(dio_df.index):
            event_data = {'name' : dio_df.loc[dio_name]['Name']}
            event_tuple = MetadataWriter._create_metadata_named_tuple(dio_name, 'event', params=event_data)
            template_dict['behavioral_events'].append(event_tuple)
        
        # Fill .yml file with electrode groups and mapping metadata        
        for electrode_id in electrode_df.index:
            coordinates = electrode_df.loc[electrode_id]['Nominal coordinates'][1:-1].split(', ')
            electrode_data = {'location' : 'hippocampus',
                              'device_type' : 'tetrode_12.5',
                              'description' : '\'tetrode\'',
                              'targeted_location' : electrode_df.loc[electrode_id]['Targeted location'],
                              'targeted_x' : coordinates[0],
                              'targeted_y' : coordinates[1],
                              'targeted_z' : coordinates[2]}
            electrode_tuple = MetadataWriter._create_metadata_named_tuple(int(electrode_id), 'electrode', params=electrode_data)
            template_dict['electrode_groups'].append(electrode_tuple)

            bad_channels = electrode_df.loc[electrode_id]['Bad channels']
            bad_channels = str(int(bad_channels)) if type(bad_channels) is np.float64 and not np.isnan(bad_channels) else bad_channels
            bad_channels = '[' + bad_channels + ']' if type(bad_channels) == str else '[]'
            map_data = {'electrode_group_id' : int(electrode_id),
                        'bad_channels' : bad_channels,
                        'map' : {'0' : 0,
                                 '1' : 1,
                                 '2' : 2,
                                 '3' : 3}}
            map_tuple = MetadataWriter._create_metadata_named_tuple(int(electrode_id), 'map', params=map_data)
            template_dict['ntrode_electrode_group_channel_map'].append(map_tuple)
        
        return template_dict


    @classmethod
    def _create_metadata_slice_names(cls):

        _metadata_column_names = {key : None for key in MetadataWriter._metadata_types}
        # Get metadata column names, index column, and labels for each metadata type
        for metadata_type in MetadataWriter._metadata_types:
            if metadata_type == 'subject':
                metadata_slice_info = {'index_name' : 'Animal name',
                                       'labels' : None,
                                       'column_names' : ['Sex', 'Genotype', 'Video scale']}
            elif metadata_type == 'session':
                metadata_slice_info = {'index_name' : 'Date',
                                       'labels' : None,
                                       'column_names' : ['Experimenters', 'Session description', 'Number of epochs']}
            elif metadata_type == 'electrode':
                metadata_slice_info = {'index_name' : 'Tetrode ID',
                                       'labels' : None,
                                       'column_names' : ['Targeted location', 'Nominal coordinates', 'Bad channels', 'Ref tetrode ID']}
            elif metadata_type == 'dio':
                metadata_slice_info = {'index_name' : 'Digital Input/Output',
                                       'labels' : None,
                                       'column_names' : ['Name']}
            else:
                raise NotImplementedError(f"Couldn't get dataframe slicing info for metadata type '{metadata_type}'")
            
            _metadata_column_names[metadata_type] = metadata_slice_info
        return _metadata_column_names

    @classmethod
    def _create_csv_file_suffixes(cls):

        # Create dictionary of .csv file name suffixes corresponding to each metadata type
        suffix_dict = {'subject' : '_subject_info.csv',
                       'session' : '_session_info.csv',
                       'electrode' : '_electrode_info.csv',
                       'dio' : '_dio_events.csv'}
        return suffix_dict

    @staticmethod
    def _get_yml_template():

        # Create template .yml file
        yml_template = {'experimenter_name' : [],
                        'lab' : 'Loren Frank',
                        'institution' : 'University of California, San Francisco',
                        'experiment_description' : None,
                        'session_description' : None,
                        'session_id' : None,
                        'subject' : {'description' : 'Long Evans Rat',
                                    'genotype' : None,
                                    'sex' : None,
                                    'species' : 'Rat',
                                    'subject_id' : None,
                                    'weight' : 'Unknown'},
                        'data_acq_device' : [],
                        'device' : {'name' : ['Trodes']},
                        'cameras' : [],
                        'associated_files' : [],
                        'associated_video_files' : [],
                        'tasks' : [],
                        'units' : {'analog' : '\'unspecified\'',
                                'behavioral_events' : '\'unspecified\''},
                        'times_period_multiplier' : '1.5',
                        'raw_data_to_volts' : '0.000000195',
                        'default_header_file_path' : 'default_header.xml',
                        'behavioral_events' : [],
                        'electrode_groups' : [],
                        'ntrode_electrode_group_channel_map' : []}

        return yml_template

    @staticmethod
    def _create_metadata_named_tuple(name, tuple_type, params=None):
        
        # Default data fields for the named tuples
        default_data_dict = {'device' : {'system' : 'MCU',
                                         'amplifier' : 'Intan',
                                         'adc_circuit' : 'Intan'},
                            'camera' : {'meters_per_pixel' : None,
                                        'manufacturer' : 'unknown',
                                        'model' : 'unknown',
                                        'lens' : 'unknown',
                                        'camera_name' : None},
                            'statescript' : {'description' : 'Statescript log',
                                             'path' : None,
                                             'task_epochs' : [None]},
                            'video' : {'camera_id' : None,
                                       'task_epoch' : None},
                            'task' : {'task_description' : None,
                                      'camera_id' : [None],
                                      'task_epochs' : [None]},
                            'event' : {'name' : None},
                            'electrode' : {'location' : None,
                                           'device_type' : None,
                                           'description' : None,
                                           'targeted_location' : None,
                                           'targeted_x' : None,
                                           'targeted_y' : None,
                                           'targeted_z' : None,
                                           'units' : '\'mm\''},
                            'map' : {'electrode_group_id' : None,
                                     'bad_channels' : None,
                                     'map' : {}}}
        
        # Replace default data fields with user-specified values
        if params != None:
            valid_keys_set = set(default_data_dict[tuple_type].keys())
            manual_keys_set = set(params.keys())
            # Ensure manual keys are valid
            dict_keys_diff = manual_keys_set.difference(valid_keys_set)
            if len(dict_keys_diff) > 0:
                raise ValueError(f"Metadata dictionary type '{tuple_type}' has no keys {list(dict_keys_diff)}")
            data = default_data_dict[tuple_type]
            for key in manual_keys_set:
                data[key] = params[key]
        # Use default data values if no user-specified values are given
        else:
            data = default_data_dict[tuple_type]
        
        named_tuple = MetadataWriter._named_tuple_dict[tuple_type](name, data)
        return named_tuple

    @function_timer
    def _write_metadata_file(self, date):

        # Ensure .yml doesn't already exist if file overwriting is disabled
        yml_files_path = self._file_info._file_name_helper._get_paths_by_file_type('yml')[0]
        yml_file_name = self._file_info.search_expected_file_names(date + '_' + self._subject_name + '.yml').full_name.tolist()[0]
        yml_file_names_list = self._file_info.search_file_names('.yml').full_name.tolist()
        if not self._overwrite and yml_file_name in yml_file_names_list:
            no_overwrite_handler_file(f"File '{yml_file_name}' already exists in'{yml_files_path}'")
            return

        # Convert metadata nested dictionary to text
        txt = self._metadata_to_text(date)
        # Write .yml file
        with open(os.path.join(yml_files_path, yml_file_name), 'w') as writer:
            writer.write(txt)
        print_verbose(self._verbose,
                      f"{os.path.join(yml_files_path, yml_file_name)}\n",
                      color='blue')
        print_verbose(self._verbose,
                      f"{txt}\n")
    

    def _metadata_to_text(self, date):

        txt = ''
        # Parse through metadata structure to create .yml file text
        txt = MetadataWriter._parse_metadata_iterable(self._metadata[date], txt, -1)
        # Trim preceding newline character
        txt = txt[1:]
        return txt

    @staticmethod
    def _parse_metadata_iterable(metadata, txt, tab):
    
        # Translate dictionary to text
        if type(metadata) is dict:
            tab += 1
            for key, value in metadata.items():
                txt += '\n'
                txt += '  '*tab + key + ':'
                txt = MetadataWriter._parse_metadata_iterable(value, txt, tab)
            tab -= 1
        # Translate list to text
        elif type(metadata) is list or type(metadata) is set or type(metadata) is tuple:
            tab += 1
            for item in metadata:
                txt += '\n'
                txt += '  '*tab + '-'
                txt = MetadataWriter._parse_metadata_iterable(item, txt, tab)
            tab -= 1
        # Translate named tuple to text
        elif hasattr(metadata, '_fields') and 'data' in metadata._fields:
            name_field = list(metadata._fields)
            name_field.remove('data')
            name_field = name_field[0]
            name_string = name_field + ': ' + str(getattr(metadata, name_field))
            txt = MetadataWriter._parse_metadata_iterable(name_string, txt, tab)
            txt = MetadataWriter._parse_metadata_iterable(getattr(metadata, 'data'), txt, tab)
        # Translate non-iterable data to text
        else:
            txt += ' ' + str(metadata)
        
        return txt