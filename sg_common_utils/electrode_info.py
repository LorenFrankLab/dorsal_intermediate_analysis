import itertools
import pandas as pd

from sg_common_utils.sg_abstract_classes import TableTools
from sg_common_utils.sg_helpers import conformed_fetch

from utils import verbose_printer
from utils.data_helpers import convert_dataframe_datatypes, transform_dataframe_values
from utils.function_wrappers import function_timer

from spyglass.common import (ElectrodeGroup, Electrode)

class ElectrodeInfo(TableTools):

    # Get relevant tables for electrode information
    _tables = {'ElectrodeGroup' : ElectrodeGroup(), 'Electrode' : Electrode()}

    def __init__(self, subject_names=None, nwb_file_names=None, verbose=False, timing=False):

        # Validate .nwb file names and query appropriate tables for electrode information
        super().__init__(subject_names=subject_names, nwb_file_names=nwb_file_names, verbose=verbose, timing=timing)
        self._electrode_groups = None
        self._electrodes = None
        self._reference_electrodes = None
        self._intact_electrodes = None
        self._dead_electrodes = None
        self._electrode_indicator = None
        self._intact_electrode_groups = None
        self._dead_electrode_groups = None
        self._electrode_group_indicator = None

    @property
    def electrode_groups(self):
        if self._electrode_groups is None:
            self._update_electrode_groups()
        return self._electrode_groups
    
    @property
    def electrodes(self):
        if self._electrodes is None:
            self._update_electrodes()
        return self._electrodes

    @property
    def reference_electrodes(self):
        if self._reference_electrodes is None:
            self._update_reference_electrodes()
        return self._reference_electrodes
    
    @property
    def intact_electrodes(self):
        if self._intact_electrodes is None:
            self._update_intact_electrodes()
        return self._intact_electrodes
    
    @property
    def dead_electrodes(self):
        if self._dead_electrodes is None:
            self._update_dead_electrodes()
        return self._dead_electrodes
    
    @property
    def electrode_indicator(self):
        if self._electrode_indicator is None:
            self._update_electrode_indicator()
        return self._electrode_indicator

    @property
    def intact_electrode_groups(self):
        if self._intact_electrode_groups is None:
            self._update_intact_electrode_groups()
        return self._intact_electrode_groups
    
    @property
    def dead_electrode_groups(self):
        if self._dead_electrode_groups is None:
            self._update_dead_electrode_groups()
        return self._dead_electrodes_groups
    
    @property
    def electrode_group_indicator(self):
        if self._electrode_group_indicator is None:
            self._update_electrode_group_indicator()
        return self._electrode_group_indicator


    def _update_electrode_groups(self):

        # Get sorted electrode group names for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_group_name']
        electrode_groups = self.fetch_as_dataframe(self._queries['ElectrodeGroup'],
                                                   attribute_names,
                                                   sort_attribute_names=attribute_names)
        # Get electrodes in each electrode group
        electrodes_dict = self.conform_by_nwb_file(self._queries,
                                                  {'Electrode' : ['electrode_id'], 'ElectrodeGroup' : ['nwb_file_name']},
                                                  {'ElectrodeGroup' : 'electrode_group_name'})
        electrodes = pd.concat(electrodes_dict.values())                
        electrode_groups['electrode_id'] = electrodes['electrode_id'].values
        electrode_groups = convert_dataframe_datatypes(electrode_groups, dtypes={'electrode_group_name' : int})
        self._electrode_groups = electrode_groups
    
    def _update_electrodes(self):

        # Get sorted electrode IDs for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id', 'electrode_group_name']
        electrodes = self.fetch_as_dataframe(self._queries['Electrode'],
                                             attribute_names,
                                             sort_attribute_names=attribute_names)
        self._electrodes = electrodes


    def _update_reference_electrodes(self):
        
        # Get reference electrode IDs for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id', 'original_reference_electrode']
        ref_electrodes = self.fetch_as_dataframe(self._queries['Electrode'],
                                                 attribute_names,
                                                 sort_attribute_names=['nwb_file_name', 'electrode_id'])
        # Get reference electrode group names for each .nwb file
        ref_group_names = self._get_reference_electrode_groups()
        ref_electrodes['reference_electrode_group_name'] = ref_group_names
        self._reference_electrodes = ref_electrodes

    def _get_reference_electrode_groups(self):
        
        # Get electrode group name of reference electrode for each electrode ID
        ref_electrode_dict = self.conform_by_nwb_file(self._queries,
                                                      {'Electrode' : ['original_reference_electrode'], 'ElectrodeGroup' : ['electrode_group_name']},
                                                      {'Electrode' : 'electrode_id'})       
        # Get reference electrode group names for each .nwb file
        ref_electrodes = pd.concat(ref_electrode_dict.values())
        ref_electrode_ids = [item[0] for item in ref_electrodes['original_reference_electrode'].values]
        ref_group_names = [item[0] for item in ref_electrodes['electrode_group_name'].values]
        #for nwb_file_ndx, ref_electrode_df in enumerate(ref_electrode_dict.values()):
        #    ref_group_names[nwb_file_ndx] = [None]*len(ref_electrode_df)
        #    # Convert electrode group names from numeric string to int
        #    ref_electrode_df = convert_dataframe_datatypes(ref_electrode_df, dtypes={'electrode_group_name' : int})
        #    # Get reference electrode group names for each electrode ID
        #    for ndx, electrode_id in enumerate(ref_electrode_df.index.values):
        #        ref_electrode_id = ref_electrode_df.loc[electrode_id]['original_reference_electrode'][0]
        #        ref_group_names[nwb_file_ndx][ndx] = ref_electrode_df.loc[ref_electrode_id]['electrode_group_name'][0]
        return ref_group_names


    def _update_intact_electrodes(self):
        
        # Get electrode IDs of intact electrodes for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id']
        query = self._queries['Electrode'] & {'bad_channel' : 'False'}
        intact_electrodes = self.fetch_as_dataframe(query,
                                                    attribute_names,
                                                    sort_attribute_names=attribute_names)
        self._intact_electrodes = intact_electrodes

    def _update_dead_electrodes(self):
        
        # Get electrode IDs of dead electrodes for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id']
        query = self._queries['Electrode'] & {'bad_channel' : 'True'}
        dead_electrodes = self.fetch_as_dataframe(query,
                                                  attribute_names,
                                                  sort_attribute_names=attribute_names)
        self._dead_electrodes = dead_electrodes
    
    def _update_electrode_indicator(self):
        
        # Get indicator of intact electrodes for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id', 'bad_channel']

        electrode_ind = self.fetch_as_dataframe(self._queries['Electrode'],
                                                attribute_names,
                                                sort_attribute_names=['nwb_file_name', 'electrode_id'])
        # Change indicator to specify intact channels instead of dead channels
        electrode_ind = ElectrodeInfo._transform_electrode_ind(electrode_ind)
        self._electrode_indicator = electrode_ind

    @staticmethod
    def _transform_electrode_ind(electrode_ind_df):
        
        # Transform 'bad_channel' column of dataframe to intact electrode indicator
        mappings = {'bad_channel' : [('True', False), ('False', True)]}
        electrode_ind_df = transform_dataframe_values(electrode_ind_df, transform_mappings=mappings)
        electrode_ind_df.rename(columns={'bad_channel' : 'intact_indicator'}, inplace=True)
        return electrode_ind_df




    def _update_electrode_group_indicator(self):
        
        # Get electrode groups for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_group_name']
        electrode_group_ind = self.fetch_as_dataframe(self._queries['ElectrodeGroup'],
                                                      attribute_names,
                                                      sort_attribute_names=attribute_names)
        # Get electrode group indicator for each .nwb file
        intact_ind, intact_channels_ind = self._get_electrode_group_indicator()
        electrode_group_ind['electrode_group_intact_indicator'] = intact_ind
        electrode_group_ind['electrode_intact_indicator'] = intact_channels_ind
        self._electrode_group_indicator = electrode_group_ind
    
    def _get_electrode_group_indicator(self):

        # Get electrode group names of electrode groups containing at least 1 intact electrode for each .nwb file
        intact_electrodes_dict = self.conform_by_nwb_file({'Electrode' : self._queries['Electrode']},
                                                          {'Electrode' : ['electrode_id', 'bad_channel']},
                                                          {'Electrode' : 'electrode_group_name'})
        # Change electrode groups indicator for each .nwb file
        intact_ind = [None]*len(intact_electrodes_dict)
        intact_channels_ind = [None]*len(intact_electrodes_dict)
        for nwb_file_ndx, group_ind_df in enumerate(intact_electrodes_dict.values()):
            intact_ind[nwb_file_ndx] = [None]*len(group_ind_df)
            intact_channels_ind[nwb_file_ndx] = [None]*len(group_ind_df)
            group_ind_df = ElectrodeInfo._transform_electrode_ind(group_ind_df)
            print(group_ind_df['intact_indicator'].values)
            # Get intact indicator for each electrode group
            for ndx, group_name in enumerate(group_ind_df.index.values):
                indicator_list = group_ind_df.loc[group_name]['intact_indicator']
                intact_channels_ind[nwb_file_ndx][ndx] = indicator_list
                intact_ind[nwb_file_ndx][ndx] = any(indicator_list)
        return list(itertools.chain(*intact_ind)), list(itertools.chain(*intact_channels_ind))


    @function_timer
    def get_electrode_group_info(self):

        # Get data conformed to electrode groups
        electrode_group_df = self._conform_queries_by_nwb_file({'ElectrodeGroup' : ['electrode_group_name', 'region_id', 'probe_id'],
                                                                'Electrode' : ['electrode_id', 'original_reference_electrode']},
                                                               {'ElectrodeGroup' : 'electrode_group_name'})
        return electrode_group_df
    
    @function_timer
    def get_electrode_info(self):

        # Get data conformed to electrodes
        electrode_df = self._conform_queries_by_nwb_file({'ElectrodeGroup' : ['electrode_group_name', 'region_id', 'probe_id'],
                                                          'Electrode' : ['electrode_id', 'original_reference_electrode', 'bad_channel']},
                                                         {'Electrode' : 'electrode_id'})
        return electrode_df
    
    @function_timer
    def get_intact_electrode_info(self):

        tables_dict = {'ElectrodeGroup' : self._queries['ElectrodeGroup'],
                       'Electrode' : self._queries['Electrode'] & {'bad_channel' : 'False'}}
        # Get data conformed to intact electrodes
        electrode_df = self._conform_by_nwb_file(tables_dict,
                                                 {'ElectrodeGroup' : ['electrode_group_name', 'region_id', 'probe_id'],
                                                  'Electrode' : ['electrode_id', 'original_reference_electrode']},
                                                 {'Electrode' : 'electrode_id'})
        return electrode_df
    
    @function_timer
    def get_dead_electrode_info(self):

        tables_dict = {'ElectrodeGroup' : self._queries['ElectrodeGroup'],
                       'Electrode' : self._queries['Electrode'] & {'bad_channel' : 'True'}}
        # Get data conformed to intact electrodes
        electrode_df = self._conform_by_nwb_file(tables_dict,
                                                 {'ElectrodeGroup' : ['electrode_group_name', 'region_id', 'probe_id'],
                                                  'Electrode' : ['electrode_id', 'original_reference_electrode']},
                                                 {'Electrode' : 'electrode_id'})
        return electrode_df
    
    def fetch_lfp_electrode_info(self, key=None, **kwargs):
        
        query = self._query_by_key(self._queries['Electrode'], key=key, **kwargs)
        lfp_electrode_df = _fetch_as_dataframe(query,
                                               ['nwb_file_name', 'electrode_group_name', 'electrode_id', 'original_reference_electrode', 'bad_channel'],
                                               sort_attribute_names=['nwb_file_name', 'electrode_group_name', 'electrode_id'],
                                               multiindex=True)
        return lfp_electrode_df


    def _update(self):
        pass


    def _print(self):
        pass