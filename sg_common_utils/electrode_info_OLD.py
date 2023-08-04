import itertools
import pandas as pd

from sg_common_utils.sg_abstract_classes import TableTools
from sg_common_utils.sg_helpers import conformed_fetch

from utils import verbose_printer
from utils.function_wrappers import function_timer
#from utils.data_helpers import 

from spyglass.common import (ElectrodeGroup, Electrode)

class ElectrodeInfo(TableTools):

    # Get relevant tables for electrode information
    _tables = {'ElectrodeGroup' : ElectrodeGroup(), 'Electrode' : Electrode()}

    def __init__(self, nwb_file_names, verbose=False, timing=False):

        # Validate .nwb file names and query appropriate tables for electrode information
        super().__init__(nwb_file_names, verbose=verbose, timing=timing)
        self._update()

    @property
    def electrode_groups(self):
        # Get sorted electrode group names for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_group_name']
        electrode_groups = self._fetch_as_dataframe(self._queries['ElectrodeGroup'],
                                                    attribute_names,
                                                    sort_attribute_names=attribute_names)
        electrode_groups = _convert_dataframe_datatypes(electrode_groups, dtypes={'electrode_group_name' : int})
        return electrode_groups
    
    @property
    def electrodes(self):
        # Get sorted electrode IDs for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id']
        electrodes = self._fetch_as_dataframe(self._queries['Electrode'],
                                              attribute_names,
                                              sort_attribute_names=attribute_names)
        return electrodes

    @property
    def reference_electrodes(self):
        # Get reference electrode IDs for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id', 'original_reference_electrode']
        ref_electrodes = self._fetch_as_dataframe(self._queries['Electrode'],
                                                  attribute_names,
                                                  sort_attribute_names=['nwb_file_name', 'electrode_id'])
        # Get reference electrode group names for each .nwb file
        ref_group_names = self._get_reference_electrode_groups()
        ref_electrodes['reference_electrode_group_name'] = ref_group_names
        return ref_electrodes

    @property
    def intact_electrodes(self):
        # Get electrode IDs of intact electrodes for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id']
        query = self._queries['Electrode'] & {'bad_channel' : 'False'}
        intact_electrodes = self._fetch_as_dataframe(query,
                                                     attribute_names,
                                                     sort_attribute_names=attribute_names)
        return intact_electrodes

    @property
    def dead_electrodes(self):
        # Get electrode IDs of dead electrodes for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id']
        query = self._queries['Electrode'] & {'bad_channel' : 'True'}
        dead_electrodes = self._fetch_as_dataframe(query,
                                                   attribute_names,
                                                   sort_attribute_names=attribute_names)
        return dead_electrodes
    
    @property
    def electrode_indicator(self):
        # Get indicator of intact electrodes for each .nwb file
        attribute_names = ['nwb_file_name', 'bad_channel']

        electrode_ind = self._fetch_as_dataframe(self._queries['Electrode'],
                                                 attribute_names,
                                                 sort_attribute_names=attribute_names)
        # Change indicator to specify intact channels instead of dead channels
        electrode_ind = ElectrodeInfo._transform_electrode_ind(electrode_ind)
        return electrode_ind
    
    @property
    def intact_electrode_groups(self):
        electrode_group_ind_list = [None]*len(self._nwb_file_names)
        # Get electrode group names of fully intact electrode groups for each .nwb file
        intact_electrodes_dict = self._conform_by_nwb_file({'Electrode' : self._queries['Electrode']},
                                                           {'Electrode' : ['electrode_id', 'bad_channel']},
                                                           {'Electrode' : 'electrode_group_name'})
        # Change dataframe to specify intact channels instead of dead channels
        for nwb_file_name in self._nwb_file_names:
            intact_electrodes_df = ElectrodeInfo._transform_electrode_ind(intact_electrodes_dict[nwb_file_name],
                                                                          column_name=('Electrode', 'bad_channel'),
                                                                          new_column_name=('Electrode', 'intact_indicator'))
            # Create dataframe of intact electrode groups
            group_ind_df = pd.DataFrame(index=intact_electrodes_df.index.get_level_values('electrode_group_name'),
                                        columns=['nwb_file_name', 'electrode_group_name', 'intact_indicator', 'intact_channel_indicator'])
            group_ind_df['nwb_file_name'] = nwb_file_name
            group_ind_df['electrode_group_name'] = intact_electrodes_df.index.get_level_values('electrode_group_name')
            #group_ind_df['intact_channel_indicator'] = intact_electrodes_df[('Electrode', 'intact_indicator')].values
            #group_ind_df['intact_indicator'] = [all(group_ind) for group_ind in intact_electrodes_df[('Electrode', 'intact_indicator')].values]
            
            return intact_electrodes_df


        #return intact_electrodes_dict[self._nwb_file_names[0]]
    
    def _update(self):
        pass

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

    def _get_reference_electrode_groups(self):
        
        # Get electrode group name of reference electrode for each electrode ID
        ref_electrode_dict = self._conform_by_nwb_file(self._queries,
                                                       {'Electrode' : ['original_reference_electrode'], 'ElectrodeGroup' : ['electrode_group_name']},
                                                       {'Electrode' : 'electrode_id'})
        ref_group_names = [None]*len(ref_electrode_dict)
        # Get reference electrode group names for each .nwb file
        for nwb_file_ndx, ref_electrode_df in enumerate(ref_electrode_dict.values()):
            ref_group_names[nwb_file_ndx] = [None]*len(ref_electrode_df)
            # Convert electrode group names from numeric string to int
            ref_electrode_df = _convert_dataframe_datatypes(ref_electrode_df, dtypes={('ElectrodeGroup', 'electrode_group_name') : int})
            # Get reference electrode group names for each electrode ID
            for ndx, electrode_id in enumerate(ref_electrode_df.index.get_level_values('electrode_id')):
                ref_electrode_id = ref_electrode_df.loc['Electrode'].loc[electrode_id]['Electrode']['original_reference_electrode'][0]
                ref_group_names[nwb_file_ndx][ndx] = ref_electrode_df.loc['Electrode'].loc[ref_electrode_id]['ElectrodeGroup']['electrode_group_name'][0]
        return list(itertools.chain(*ref_group_names))
    

    @staticmethod
    def _transform_electrode_ind(electrode_ind_df, column_name='bad_channel', new_column_name='intact_indicator'):
        # Transform 'bad_channel' column of dataframe to intact electrode indicator
        mappings = {column_name : [('True', False), ('False', True)]}
        electrode_ind_df = _transform_dataframe_values(electrode_ind_df, transform_mappings=mappings)
        #electrode_ind_df.columns.set_levels(columns={column_name : new_column_name}, inplace=True)
        return electrode_ind_df