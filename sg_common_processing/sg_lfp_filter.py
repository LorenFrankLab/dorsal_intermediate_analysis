import numpy as np

from sg_common_utils.electrode_info import ElectrodeInfo
from sg_common_utils.preprocessing_info import PreprocessingInfo
from sg_common_utils.sg_abstract_classes import TableWriter
from sg_common_utils.sg_helpers import (fetch,
                                        query_by_key,
                                        sql_or_query)
from sg_common_utils.sg_metadata_helpers import (get_ephys_sampling_rate,
                                                 get_lfp_sampling_rate)
from sg_common_utils.sg_tables import LFP, LFPBand, LFPBandSelection, LFPElectrodeGroup, LFPMerge, LFPOutput, LFPSelection

from utils.file_helpers import no_overwrite_handler_table
from utils.function_wrappers import function_timer

class LFPFilter(TableWriter):

    _merged_lfp_electrode_group_name = 'all_intact_tetrodes'

    _tables = {'LFPElectrodeGroup' : LFPElectrodeGroup(),
               'LFPElectrode' : LFPElectrodeGroup.LFPElectrode(),
               'LFPSelection' : LFPSelection(),
               'LFP' : LFP(),
               'LFPOutput' : LFPOutput(),
               'LFPBandElectrode' : LFPBandSelection.LFPBandElectrode(),
               'LFPBandSelection' : LFPBandSelection(),
               'LFPBand' : LFPBand()}
    _filter_names = ['Theta 5-11 Hz', 'Ripple 150-250 Hz']
    _use_ref_electrodes = {'Theta 5-11 Hz' : False, 'Ripple 150-250 Hz' : True}

    def __init__(self, subject_names=None, nwb_file_names=None, interval_list_names=None, overwrite=False, verbose=False, timing=False):

        # Validate .nwb file names and query appropriate tables for electrode information
        super().__init__(subject_names=subject_names, nwb_file_names=nwb_file_names, interval_list_names=interval_list_names, overwrite=overwrite, verbose=verbose, timing=timing)
        # Get preprocessing analyses info
        self._preprocessing_info = PreprocessingInfo(subject_names=self.subject_names)
        # Get electrodes info
        self._electrode_info = ElectrodeInfo(subject_names=self.subject_names)

    @function_timer
    def lfp_filter(self):

        self._update()
        # Filter electrophysiology data in each .nwb file
        for nwb_file_name in self.nwb_file_names:
            self._create_lfp_filtered_data(nwb_file_name)
    
    @function_timer
    def depopulate_lfp(self):

        # Remove all LFP and LFP band entries and electrode entries
        for nwb_file_name in self.nwb_file_names:
            # Remove merge entry
            merge_ids = fetch(LFPOutput & {'nwb_file_name' : nwb_file_name}, 'merge_id')
            for merge_id in merge_ids:
                LFPFilter.delete_lfp_output({'merge_id' : merge_id})
        
        key = sql_or_query('nwb_file_name', self.nwb_file_names)
        LFPFilter.delete_lfp_electrode_group(key)
        LFPFilter.delete_lfp_selection(key)
        LFPFilter.delete_lfp(key)
        LFPFilter.delete_lfp_band_selection(key)
        LFPFilter.delete_lfp_band(key)

    @function_timer
    def _create_lfp_filtered_data(self, nwb_file_name):

        # Create LFP filter electrode groups if they don't exist already
        self._create_merged_lfp_electrode_group(nwb_file_name)
        self._lfp_filter_data(nwb_file_name)
        self._create_lfp_band_electrode_groups(nwb_file_name)
        self._lfp_band_filter_data(nwb_file_name)

    def _create_merged_lfp_electrode_group(self, nwb_file_name):

        lfp_electrodes_df = self._get_lfp_electrodes(nwb_file_name)
        # Create a key for an LFP electrode group containing all LFP electrodes
        key = {'nwb_file_name' : nwb_file_name,
               'electrode_group_name' : LFPFilter._merged_lfp_electrode_group_name,
               'electrode_id' : list(lfp_electrodes_df['electrode_id'].values)}
        
        # Check if LFP electrode group entry already exists
        electrode_group_exists_bool = self.validate_table_entry('LFPElectrode', key)
        if not self._overwrite and electrode_group_exists_bool:
            no_overwrite_handler_table(f"LFP electrode group already exists")
        else:
            LFPFilter.insert_lfp_electrode_group(key)

    def _create_lfp_electrode_groups(self, nwb_file_name):

        lfp_electrodes_df = self._get_lfp_electrodes(nwb_file_name)
        # Create a key for each LFP electrode group
        for _, df_row in lfp_electrodes_df.iterrows():
            key = {'nwb_file_name' : nwb_file_name,
                   'electrode_group_name' : df_row['electrode_group_name'],
                   'electrode_id' : [df_row['electrode_id']]}
            
            # Check if LFP electrode group entry already exists
            electrode_group_exists_bool = self.validate_table_entry('LFPElectrode', key)
            if not self._overwrite and electrode_group_exists_bool:
                no_overwrite_handler_table(f"LFP electrode group already exists")
            else:
                LFPFilter.insert_lfp_electrode_group(key)

    def _lfp_filter_data(self, nwb_file_name):
        
        sampling_rate = int(get_ephys_sampling_rate(nwb_file_name))
        # Filter data for each interval list for the given .nwb file
        for interval_list_name in self.interval_list_names[nwb_file_name]:
            # Filter data for each LFP electrode group
            lfp_electrode_groups = fetch(self.queries['LFPElectrodeGroup'], 'lfp_electrode_group_name')
            for lfp_electrode_group in lfp_electrode_groups:
                
                # Create LFP selection
                selection_key = {'nwb_file_name' : nwb_file_name,
                                 'lfp_electrode_group_name' : lfp_electrode_group,
                                 'target_interval_list_name' : interval_list_name,
                                 'filter_name' : 'LFP 0-400 Hz',
                                 'filter_sampling_rate' : sampling_rate}
                
                # Check if LFP selection entry already exists
                selection_exists_bool = self.validate_table_entry('LFPSelection', selection_key)
                if not self._overwrite and selection_exists_bool:
                    no_overwrite_handler_table(f"LFP selection already exists")
                else:
                    LFPFilter.insert_lfp_selection(selection_key)
                
                # Low pass filter raw ephys data
                lfp_exists_bool = self.validate_table_entry('LFPSelection', selection_key)
                if not self._overwrite and lfp_exists_bool:
                    no_overwrite_handler_table(f"LFP selection already exists")
                else:
                    LFPFilter.populate_lfp(selection_key)

    def _create_lfp_band_electrode_groups(self, nwb_file_name):

        lfp_sampling_rate = get_lfp_sampling_rate(nwb_file_name)
        lfp_electrodes_df = self._get_lfp_electrodes(nwb_file_name)
        # Create LFP band filtering electrode groups for each interval list
        for interval_list_name in self.interval_list_names[nwb_file_name]:
            lfp_electrode_groups = fetch(self.queries['LFPElectrodeGroup'], 'lfp_electrode_group_name')
            for lfp_electrode_group in lfp_electrode_groups:
                query = self.queries['LFPElectrode'] & {'nwb_file_name' : nwb_file_name, 'lfp_electrode_group_name' : lfp_electrode_group}
                electrode_ids = fetch(query, 'electrode_id')
                # Create a key for each LFP band electrode group
                key = {'nwb_file_name' : nwb_file_name,
                       'target_interval_list_name' : interval_list_name,
                       'electrode_id' : electrode_ids,
                       'lfp_band_sampling_rate' : lfp_sampling_rate}
                key['lfp_merge_id'] = self._get_lfp_merge_id(nwb_file_name, interval_list_name, lfp_electrode_group)
                
                for filter_name in LFPFilter._filter_names:
                    key['filter_name' ] = filter_name
                    # Set reference electrodes
                    if LFPFilter._use_ref_electrodes[filter_name]:
                        key['reference_elect_id'] = list( lfp_electrodes_df[lfp_electrodes_df['electrode_id'].isin(electrode_ids)]['original_reference_electrode'].values )
                    else:
                        # Don't use a reference electrode for LFP band filtering
                        key['reference_elect_id'] = [-1]
                    
                    # Check if LFP band electrode group entry already exists
                    electrode_group_exists_bool = self.validate_table_entry('LFPBandElectrode', key)
                    if not self._overwrite and electrode_group_exists_bool:
                        no_overwrite_handler_table(f"LFP band electrode group already exists")
                    else:
                        LFPFilter.insert_lfp_band_electrode_group(key)

    def _lfp_band_filter_data(self, nwb_file_name):

        # Bandpass filter data for each interval list for the given .nwb file
        for interval_list_name in self.interval_list_names[nwb_file_name]:
            # Filter data for each LFP band electrode group
            lfp_electrode_groups = fetch(self.queries['LFPElectrodeGroup'], 'lfp_electrode_group_name')
            for lfp_electrode_group in lfp_electrode_groups:
                
                # Create LFP band selection key
                key = {'nwb_file_name' : nwb_file_name,
                       'target_interval_list_name' : interval_list_name}
                key['lfp_merge_id'] = self._get_lfp_merge_id(nwb_file_name, interval_list_name, lfp_electrode_group)
                
                for filter_name in LFPFilter._filter_names:

                    key['filter_name'] = filter_name
                    # Bandpass filter LFP-filtered data
                    lfp_band_exists = self.validate_table_entry('LFPBand', key)
                    if not self._overwrite and lfp_band_exists:
                        no_overwrite_handler_table(f"LFP band already exists")
                    else:
                        LFPFilter.populate_lfp_band(key)


    @staticmethod
    def insert_lfp_electrode_group(key):

        # Create LFP electrode group
        LFPElectrodeGroup.create_lfp_electrode_group(nwb_file_name=key['nwb_file_name'],
                                                     group_name=key['electrode_group_name'],
                                                     electrode_list=key['electrode_id'])

    @staticmethod
    def insert_lfp_selection(key):

        # Create LFP selection
        LFPSelection.insert1(key, skip_duplicates=True)
    
    @staticmethod
    def populate_lfp(key):

        # Low pass filter raw ephys data
        LFP.populate(key)
    
    @staticmethod
    def insert_lfp_band_electrode_group(key):

        # Create LFP band electrode group and LFP band selection
        LFPBandSelection().set_lfp_band_electrodes(nwb_file_name=key['nwb_file_name'],
                                                   lfp_merge_id=key['lfp_merge_id'],
                                                   electrode_list=key['electrode_id'],
                                                   filter_name=key['filter_name'],
                                                   interval_list_name=key['target_interval_list_name'],
                                                   reference_electrode_list=key['reference_elect_id'],
                                                   lfp_band_sampling_rate=key['lfp_band_sampling_rate'])
    
    @staticmethod
    def populate_lfp_band(key):

        # Bandpass filter LFP-filtered data
        LFPBand.populate(LFPBandSelection & key)

    
    @staticmethod
    def delete_lfp_output(key):

        # Delete LFP output
        (LFPMerge & key).delete()

    @staticmethod
    def delete_lfp_electrode_group(key):

        # Delete LFP electrode group
        (LFPElectrodeGroup & key).delete()

    @staticmethod
    def delete_lfp_selection(key):

        # Delete LFP selection
        (LFPSelection & key).delete()

    @staticmethod
    def delete_lfp(key):

        # Delete LFP
        (LFP & key).delete()
    
    @staticmethod
    def delete_lfp_band_selection(key):

        # Delete LFP band selection
        (LFPBandSelection & key).delete()
    
    @staticmethod
    def delete_lfp_band(key):

        # Delete LFP band
        (LFPBand & key).delete()


    def _get_lfp_electrodes(self, nwb_file_name):

        # Get LFP electrode info for the given .nwb file
        lfp_electrodes_df = self._electrode_info.lfp_electrodes
        lfp_electrodes_df = lfp_electrodes_df[lfp_electrodes_df['nwb_file_name'] == nwb_file_name]
        return lfp_electrodes_df

    def _get_lfp_merge_id(self, nwb_file_name, interval_list_name, lfp_electrode_group_name):

        key = {'nwb_file_name' : nwb_file_name,
               'lfp_electrode_group_name' : lfp_electrode_group_name,
               'target_interval_list_name' : interval_list_name}
        merge_id = fetch(query_by_key(LFPOutput, key), 'merge_id')
        assert len(merge_id) == 1, \
            f"Didn't find exactly 1 merge ID for {nwb_file_name}, {interval_list_name}, LFP electrode group {lfp_electrode_group_name}"
        return merge_id[0]


    def _update(self):
        pass
    
    def _print(self):
        pass