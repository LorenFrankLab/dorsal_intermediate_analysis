import itertools
import pandas as pd

from sg_common_utils.sg_abstract_classes import TableTools
from sg_common_utils.sg_helpers import (fetch_as_dataframe,
                                        multi_table_fetch)

from utils import verbose_printer
from utils.data_helpers import convert_dataframe_datatypes, transform_dataframe_values, unique_dataframe
from utils.function_wrappers import function_timer

from spyglass.common import (ElectrodeGroup, Electrode)

class ElectrodeInfo(TableTools):

    # Get relevant tables for electrode information
    _tables = {'ElectrodeGroup' : ElectrodeGroup(), 'Electrode' : Electrode()}

    def __init__(self, subject_names=None, nwb_file_names=None, verbose=False, timing=False):

        # Validate .nwb file names and query appropriate tables for electrode information
        super().__init__(subject_names=subject_names, nwb_file_names=nwb_file_names, verbose=verbose, timing=timing)
        self._electrodes = None
        self._reference_electrodes = None
        self._intact_electrodes = None
        self._dead_electrodes = None
        self._electrode_indicator = None
        self._lfp_electrode = None
    
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
    def lfp_electrode(self):
        if self._lfp_electrode is None:
            self._update_lfp_electrode()
        return self._lfp_electrode
    
    def _update_electrodes(self):

        # Get sorted electrode IDs for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id', 'electrode_group_name']
        electrodes = fetch_as_dataframe(self._queries['Electrode'],
                                        attribute_names,
                                        sort_attribute_names=attribute_names)
        self._electrodes = electrodes

    def _update_reference_electrodes(self):
        
        # Get reference electrode IDs for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id', 'original_reference_electrode']
        ref_electrodes = fetch_as_dataframe(self._queries['Electrode'],
                                            attribute_names,
                                            sort_attribute_names=['nwb_file_name', 'electrode_id'])
        self._reference_electrodes = ref_electrodes

    def _update_intact_electrodes(self):
        
        # Get electrode IDs of intact electrodes for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id']
        query = self._queries['Electrode'] & {'bad_channel' : 'False'}
        intact_electrodes = fetch_as_dataframe(query,
                                               attribute_names,
                                               sort_attribute_names=attribute_names)
        self._intact_electrodes = intact_electrodes

    def _update_dead_electrodes(self):
        
        # Get electrode IDs of dead electrodes for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id']
        query = self._queries['Electrode'] & {'bad_channel' : 'True'}
        dead_electrodes = fetch_as_dataframe(query,
                                             attribute_names,
                                             sort_attribute_names=attribute_names)
        self._dead_electrodes = dead_electrodes
    
    def _update_electrode_indicator(self):
        
        # Get indicator of intact electrodes for each .nwb file
        attribute_names = ['nwb_file_name', 'electrode_id', 'bad_channel']

        electrode_ind = fetch_as_dataframe(self._queries['Electrode'],
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
    
    def _update_lfp_electrode(self):
        
        # Get first intact electrode of each electrode group
        attribute_names = ['nwb_file_name', 'electrode_group_name', 'electrode_id']
        query = self._queries['Electrode'] & {'bad_channel' : 'False'}
        lfp_electrodes = self.fetch_by_nwb_file(query,
                                                attribute_names,
                                                sort_attribute_names=['nwb_file_name', 'electrode_id'])
        for nwb_file_name, lfp_electrode_df in lfp_electrodes.items():
            unique_df, _ = unique_dataframe(lfp_electrode_df, 'electrode_group_name')
            lfp_electrodes[nwb_file_name] = unique_df
        self._lfp_electrode = pd.concat(lfp_electrodes.values())


    def _update(self):
        self._update_electrodes()
        self._update_reference_electrodes()
        self._update_intact_electrodes()
        self._update_dead_electrodes()
        self._update_electrode_indicator()
        self._update_lfp_electrode()

    def _print(self):
        pass