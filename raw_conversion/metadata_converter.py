from collections import namedtuple
import numpy as np
import pandas as pd
import re

from .file_info import FileInfo

from .file_name_helper import FileNameHelper

from utils import verbose_printer
from utils.abstract_classes import DataReader

class MetadataConverter(DataReader):

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

    def __init__(self, subject_name, dates=None):

        # Enable overwriting existing .yml files, verbose output, and timing
        super().__init__(subject_name, dates=dates, verbose=False, timing=False)

        # Get file information for raw data and metadata
        self._file_info = FileInfo(self.subject_name, dates=self.dates)       
        # Get index column name and labels and column names used to slice metadata
        self._metadata_slice_names = MetadataConverter._create_metadata_slice_names()
        # Get only metadata for the specified subject and dates
        self._metadata_slice_names['subject']['labels'] = self.subject_name
        self._metadata_slice_names['session']['labels'] = self.dates
        self._metadata = None

    @property
    def metadata(self):
        if self._metadata is None:
            self._update()
        return self._metadata
    

    def metadata_to_text(self, date):

        self._update()
        txt = ''
        # Parse through metadata structure to create .yml file text
        txt = MetadataConverter._parse_metadata_iterable(self._metadata[date], txt, -1)
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
                txt = MetadataConverter._parse_metadata_iterable(value, txt, tab)
            tab -= 1
        # Translate list to text
        elif type(metadata) is list or type(metadata) is set or type(metadata) is tuple:
            tab += 1
            for item in metadata:
                txt += '\n'
                txt += '  '*tab + '-'
                txt = MetadataConverter._parse_metadata_iterable(item, txt, tab)
            tab -= 1
        # Translate named tuple to text
        elif hasattr(metadata, '_fields') and 'data' in metadata._fields:
            name_field = list(metadata._fields)
            name_field.remove('data')
            name_field = name_field[0]
            name_string = name_field + ': ' + str(getattr(metadata, name_field))
            txt = MetadataConverter._parse_metadata_iterable(name_string, txt, tab)
            txt = MetadataConverter._parse_metadata_iterable(getattr(metadata, 'data'), txt, tab)
        # Translate non-iterable data to text
        else:
            txt += ' ' + str(metadata)
        
        return txt
        

    def _get_csv_file_names(self):

        # Get full names of .csv metadata files from FileInfo dataframes
        suffix_dict = MetadataConverter._create_csv_file_suffixes()
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
    
    @classmethod
    def _create_csv_file_suffixes(cls):

        # Create dictionary of .csv file name suffixes corresponding to each metadata type
        suffix_dict = {'subject' : '_subject_info.csv',
                       'session' : '_session_info.csv',
                       'electrode' : '_electrode_info.csv',
                       'dio' : '_dio_events.csv'}
        return suffix_dict


    def _create_yml_metadata(self):

        metadata = {}
        # Get metadata for each date
        for date in self.dates:
            metadata[date] = self._parse_metadata(date)
        return metadata
    
    def _parse_metadata(self, date):

        subject_df = self._csv_metadata['subject']
        session_df = self._csv_metadata['session'].loc[date]
        electrode_df = self._csv_metadata['electrode']
        dio_df = self._csv_metadata['dio']
        # Get metadata template
        template_dict = MetadataConverter._get_yml_template()
        
        # Determine session ID, number of epochs in recording, and task type
        session_id = self.get_session_dates().index(date)+1
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
            raise ValueError(f"Unable to determine task type from session description '{session_description}' on '{date}_{self.subject_name}'")

        # Generate epoch-specific metadata       
        task_dict = {}
        task_dict['epoch_numbers_list'] = list(range(1, n_epochs+1))
        # Get sorted statescript file names and full paths
        task_dict['statescript_file_names_list'] = sorted(self._file_info.search_matching_file_names('^' + date + '.*.stateScriptLog$').full_name.tolist())
        task_dict['statescript_names_list'] = ['statescript_' + FileNameHelper.get_epoch_name_info(ndx+1)['epoch_name'][0] for ndx in range(self._n_epochs[self.dates.index(date)])]
        # Get video file and camera names
        task_dict['video_file_names_list'] = sorted(self._file_info.search_matching_file_names('^' + date + '.*.h264$').file_name.tolist())
        task_dict['camera_names_list'] = [FileNameHelper.get_epoch_name_info(epoch)['camera_name'][0] for epoch in range(1, n_epochs+1)]

        # Fill .yml file template with subject and session metadata
        template_dict['experimenter_name'] = session_df['Experimenters'].split(', ')
        template_dict['experiment_description'] = 'Four-box, six-arm automated track'
        template_dict['session_description'] = session_description
        template_dict['session_id'] = session_id
        template_dict['subject']['genotype'] = subject_df['Genotype']
        template_dict['subject']['sex'] = subject_df['Sex']
        template_dict['subject']['subject_id'] = self.subject_name

        # Fill .yml file template with acquisition metadata
        device_tuple = MetadataConverter._create_metadata_named_tuple('trodes', 'device')
        template_dict['data_acq_device'].append(device_tuple)
        
        # Fill .yml file template with camera metadata
        for ndx, camera_name in enumerate(set(task_dict['camera_names_list'])):
            camera_data = {'meters_per_pixel' : float(subject_df['Video scale'].split(' ')[0]),
                           'camera_name' : camera_name}
            camera_tuple = MetadataConverter._create_metadata_named_tuple(ndx, 'camera', params=camera_data)
            template_dict['cameras'].append(camera_tuple)
        
        # Fill .yml file template with statescript log and video file metadata
        for ndx in range(n_epochs):
            statescript_data = {'path' : task_dict['statescript_file_names_list'][ndx],
                                'task_epochs' : [ndx+1]}
            statescript_tuple = MetadataConverter._create_metadata_named_tuple(task_dict['statescript_names_list'][ndx], 'statescript', params=statescript_data)
            template_dict['associated_files'].append(statescript_tuple)
            
            camera_name = task_dict['camera_names_list'][ndx]
            video_data = {'camera_id' : list(set(task_dict['camera_names_list'])).index(camera_name),
                          'task_epoch' : task_dict['epoch_numbers_list'][ndx]}
            video_tuple = MetadataConverter._create_metadata_named_tuple(task_dict['video_file_names_list'][ndx], 'video', params=video_data)
            template_dict['associated_video_files'].append(video_tuple)
        
        # Fill .yml file template with task metadata
        camera_names_set = set(task_dict['camera_names_list'])
        camera_ids_list = [task_dict['camera_names_list'].index(name) for name in camera_names_set]
        task_data = {'task_description' : task_description,
                     'camera_id' : camera_ids_list,
                     'task_epochs' : task_dict['epoch_numbers_list']}
        task_tuple = MetadataConverter._create_metadata_named_tuple(task_name, 'task', params=task_data)
        template_dict['tasks'].append(task_tuple)

        # Fill .yml file with behavioral events (DIO) metadata
        for ndx, dio_name in enumerate(dio_df.index):
            event_data = {'name' : dio_df.loc[dio_name]['Name']}
            event_tuple = MetadataConverter._create_metadata_named_tuple(dio_name, 'event', params=event_data)
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
            electrode_tuple = MetadataConverter._create_metadata_named_tuple(int(electrode_id), 'electrode', params=electrode_data)
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
            map_tuple = MetadataConverter._create_metadata_named_tuple(int(electrode_id), 'map', params=map_data)
            template_dict['ntrode_electrode_group_channel_map'].append(map_tuple)
        
        return template_dict


    @classmethod
    def _create_metadata_slice_names(cls):

        _metadata_column_names = {key : None for key in MetadataConverter._metadata_types}
        # Get metadata column names, index column, and labels for each metadata type
        for metadata_type in MetadataConverter._metadata_types:
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
        
        named_tuple = MetadataConverter._named_tuple_dict[tuple_type](name, data)
        return named_tuple


    def _update(self):
        
        # Update file names, expected file names, and missing and matching file names
        self._file_info.update()
        # Read .csv metadata into dataframes
        self._csv_file_names = self._get_csv_file_names()
        self._csv_metadata = self._get_csv_metadata()
        # Parse metadata to nested dictionaries that will be converted to .yml files
        self._metadata = self._create_yml_metadata()


    def _print(self, dates):

        # Print metadata for the given dates
        for date in dates:
            verbose_printer.print_header(f"{self.subject_name} {date} .yml file metadata text",
                                         f"{self._csv_file_names['subject']}",
                                         f"{self._csv_file_names['session']}",
                                         f"{self._csv_file_names['electrode']}",
                                         f"{self._csv_file_names['dio']}")
            metadata_txt = self.metadata_to_text(date)
            verbose_printer.print_text(f"{metadata_txt}")
            verbose_printer.print_newline()