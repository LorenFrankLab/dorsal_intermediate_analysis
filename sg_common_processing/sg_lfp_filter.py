from sg_common_utils.electrode_info import ElectrodeInfo
from sg_common_utils.preprocessing_info import PreprocessingInfo
from sg_common_utils.sg_abstract_classes import TableWriter
from sg_common_utils.sg_helpers import fetch_as_dataframe

from utils.data_helpers import transform_dataframe_values, unique_dataframe
from utils.file_helpers import no_overwrite_handler_table
from utils.function_wrappers import function_timer

from spyglass.lfp import LFPElectrodeGroup
from spyglass.lfp.v1 import (LFPSelection, LFPV1)
from spyglass.lfp.analysis.v1.lfp_band import (LFPBandSelection, LFPBandV1)

class LFPFilter(TableWriter):

    _tables = {'LFPElectrodeGroup' : LFPElectrodeGroup,
               'LFPSelection' : LFPSelection,
               'LFP' : LFPV1,
               'LFPBandSelection' : LFPBandSelection,
               'LFPBand' : LFPBandV1}

    def __init__(self, subject_names=None, nwb_file_names=None, verbose=False, timing=False):

        # Validate .nwb file names and query appropriate tables for electrode information
        super().__init__(subject_names=subject_names, nwb_file_names=nwb_file_names, verbose=verbose, timing=timing)
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
    def _create_lfp_filtered_data(self, nwb_file_name):

        # Create LFP filter electrode groups if they don't exist already
        self._create_lfp_electrode_groups(nwb_file_name)



    def _create_lfp_electrode_groups(self, nwb_file_name):

        # Get LFP electrode info for the given .nwb file
        lfp_electrodes_df = self._electrode_info.lfp_electrode
        lfp_electrodes_df = lfp_electrodes_df[lfp_electrodes_df['nwb_file_name'] == nwb_file_name]
        # Create a key for each LFP electrode group
        for df_row in lfp_electrodes_df.iterrows():
            key = {'nwb_file_name' : nwb_file_name,
                   'electrode_group_name' : df_row['electrode_group_name'],
                   'electrode_id' : df_row['electrode_group_name']}
            # Check if LFP electrode group entry already exists
            entry_exists_bool = self.validate_table_entry('LFPElectrodeGroup', key)
            if not self._overwrite and entry_exists_bool:
                no_overwrite_handler_table(f"LFP electrode group already exists")
        else:
            # Create LFP electrode goup
            self.tables['LFPElectrodeGroup'].create_lfp_electrode_group(nwb_file_name=key['nwb_file_name'],
                                                                        group_name=key['electrode_group_name'],
                                                                        electrode_list=[key['electrode_id']])






def lfp_filter(nwb_file_name, lfp_electrodes_list):

    # Create LFP selection
    lfp_key = dict()
    lfp_key['nwb_file_name'] = nwb_file_name
    
    LFPSelection().set_lfp_electrodes(nwb_file_name, lfp_electrodes_list)
    LFP.populate([(LFPSelection & lfp_key).proj()])

def theta_band_filter(nwb_file_name, lfp_electrodes_list, ref_electrodes_list=None):
    
    # Filter using default theta band filter
    filter_name = 'Theta 5-11 Hz'
    _lfp_band_filter(nwb_file_name, filter_name, lfp_electrodes_list, ref_electrodes_list)

def ripple_band_filter(nwb_file_name, lfp_electrodes_list, ref_electrodes_list=None):
    
    # Filter using default ripple band filter
    filter_name = 'Ripple 150-250 Hz'
    _lfp_band_filter(nwb_file_name, filter_name, lfp_electrodes_list, ref_electrodes_list)

def _lfp_band_filter(nwb_file_name, filter_name, lfp_electrodes_list, ref_electrodes_list):
   
    # Create LFP band selection filtering
    lfp_band_key = dict()
    lfp_band_key['nwb_file_name'] = nwb_file_name
    lfp_band_key['filter_name'] = filter_name
    lfp_band_key['target_interval_list_name'] = 'raw data valid times'
    lfp_band_key['lfp_band_sampling_rate'] = (LFP() & {'nwb_file_name' : nwb_file_name}).fetch1('lfp_sampling_rate')
    # Turn reference electrodes off if no reference electrodes are specified
    if ref_electrodes_list is None:
        ref_electrodes_list = [-1]*len(lfp_electrodes_list)
    
    LFPBandSelection().set_lfp_band_electrodes(nwb_file_name,
                                               lfp_electrodes_list,
                                               lfp_band_key['filter_name'],
                                               lfp_band_key['target_interval_list_name'],
                                               ref_electrodes_list,
                                               lfp_band_key['lfp_band_sampling_rate'])
    LFPBand.populate([(LFPBandSelection & lfp_band_key).proj()])
