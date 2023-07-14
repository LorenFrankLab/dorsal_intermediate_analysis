from spyglass.common import (ElectrodeGroup, Electrode,
                             LFPSelection, LFP, LFPBandSelection, LFPBand)

def get_lfp_electrodes(nwb_file_name, electrode_groups_list=None, verbose=False):

    # Filter all electrode groups if no groups are specified
    if electrode_groups_list is None:
        electrode_groups_list = (ElectrodeGroup() & {'nwb_file_name' : nwb_file_name}).fetch('electrode_group_name').tolist()
    n_electrode_groups = len(electrode_groups_list)
    # List of which electrode to use for LFP filtering within each electrode group
    lfp_electrodes_list = [None]*n_electrode_groups
    # List of reference electrodes for the electrodes that will be LFP filtered
    ref_electrodes_list = [None]*n_electrode_groups

    intact_ind = [True]*n_electrode_groups
    # For each electrode group, use the first non-dead electrode for LFP filtering
    for ndx, group_name in enumerate(electrode_groups_list):
        # List of all intact electrodes for the given electrode group
        intact_electrodes_list = (Electrode() & {'nwb_file_name' : nwb_file_name,
                                                 'electrode_group_name' : group_name,
                                                 'bad_channel' : 'False'}).fetch('electrode_id')
        # Skip if electrode group has only dead channels
        if len(intact_electrodes_list) == 0:
            intact_ind[ndx] = False
            continue
        
        # Use the first non-dead electrode for LFP filtering
        lfp_electrode = intact_electrodes_list[0]
        lfp_electrodes_list[ndx] = lfp_electrode
        # Get the reference electrode for the selected LFP electrode
        ref_electrodes_list[ndx] = (Electrode() & {'nwb_file_name' : nwb_file_name,
                                                   'electrode_id' : lfp_electrode}).fetch1('original_reference_electrode')
    if verbose:
        for group, chn, ref, ind in zip(electrode_groups_list, lfp_electrodes_list, ref_electrodes_list, intact_ind):
            if ind:
                print(f"Electrode group '{group}', electrode '{chn}', reference electrode '{ref}'")
            else:
                print(f"No intact electrodes on electrode group '{group}'")
    # Remove any dead electrode groups
    for elec_list in (electrode_groups_list, lfp_electrodes_list, ref_electrodes_list):
        elec_list = [item for item, ind in zip(elec_list, intact_ind) if ind]

    return electrode_groups_list, lfp_electrodes_list, ref_electrodes_list

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
