from email.policy import default
import os

from user_settings import team_name
from spyglass.common import (ElectrodeGroup, Electrode,
                             IntervalList)
from spyglass.spikesorting import (SpikeSortingPreprocessingParameters,
                                   SortGroup, SortInterval,
                                   SpikeSortingRecordingSelection, SpikeSortingRecording,
                                   ArtifactDetectionParameters,
                                   ArtifactDetection,
                                   ArtifactDetectionSelection, ArtifactRemovedIntervalList,
                                   SpikeSorterParameters,
                                   SpikeSortingSelection, SpikeSorting)

def make_sort_groups(nwb_file_name, sort_groups_list=None, verbose=False):

    # Spike sort all electrode groups list if no groups are specified
    if sort_groups_list is None:
        sort_groups_list = (ElectrodeGroup() & {'nwb_file_name' : nwb_file_name}).fetch('electrode_group_name').tolist()
    n_electrode_groups = len(sort_groups_list)
    # List of which electrodes constitute each sort group
    electrodes_list = [None]*n_electrode_groups
    # List of reference electrodes for the sort groups
    ref_electrodes_list = [None]*n_electrode_groups

    intact_ind = [True]*n_electrode_groups
    # For each sort group, use the the non-dead electrodes for spike sorting
    for ndx, group_name in enumerate(sort_groups_list):
        # List of all intact electrodes for the given electrode group
        intact_electrodes_list = (Electrode() & {'nwb_file_name' : nwb_file_name,
                                                'electrode_group_name' : group_name,
                                                'bad_channel' : 'False'}).fetch('electrode_id')
        # Skip if sort group has only dead channels
        if len(intact_electrodes_list) == 0:
            intact_ind[ndx] = False
            continue
        
        # Use non-dead electrodes for spike sorting
        electrodes_list[ndx] = intact_electrodes_list
        # Get the reference electrode for the selected LFP electrode
        ref_electrodes_list[ndx] = (Electrode() & {'nwb_file_name' : nwb_file_name,
                                                   'electrode_id' : intact_electrodes_list[0]}).fetch1('original_reference_electrode')
    if verbose:
        for group, chn_list, ref, ind in zip(sort_groups_list, electrodes_list, ref_electrodes_list, intact_ind):
            if ind:
                print(f"Sort group '{group}', electrodes '{chn_list}', reference electrode '{ref}'")
            else:
                print(f"No intact electrodes on sort group '{group}'")
    # Remove any dead electrode groups
    for elec_list in (sort_groups_list, electrodes_list, ref_electrodes_list):
        elec_list = [item for item, ind in zip(elec_list, intact_ind) if ind]

    # Insert sort groups into SortGroup table and SortGroup.SortGroupElectrode parts table
    (SortGroup & {'nwb_file_name' : nwb_file_name}).delete()
    for group, chn_list, ref in zip(sort_groups_list, electrodes_list, ref_electrodes_list):
        SortGroup.insert1({'nwb_file_name' : nwb_file_name,
                           'sort_group_id' : group,
                           'sort_reference_electrode_id' : ref})
        for chn in chn_list:
            SortGroup.SortGroupElectrode.insert1({'nwb_file_name' : nwb_file_name,
                                                  'sort_group_id' : group,
                                                  'electrode_group_name' : group,
                                                  'electrode_id' : chn})
    
    return sort_groups_list, electrodes_list, ref_electrodes_list

def make_sort_interval(nwb_file_name, sort_interval_name=None):

    # If no interval list is specified, sort entire session
    if sort_interval_name is None:
        sort_interval_name = 'raw data valid times'
    sort_interval = (IntervalList & {'nwb_file_name' : nwb_file_name,
                                     'interval_list_name' : sort_interval_name}).fetch1('valid_times')
    SortInterval.insert1({'nwb_file_name' : nwb_file_name,
                          'sort_interval_name' : sort_interval_name,
                          'sort_interval' : sort_interval})

def make_spike_sorting_recording(nwb_file_name, spike_sorter_parameter_set_name='mountainsort4', sort_groups_list=None, sort_interval_name=None):

    # If no sort groups specified, use all sort groups
    if sort_groups_list is None:
        sort_groups_list = (SortGroup & {'nwb_file_name' : nwb_file_name}).fetch('sort_group_id')
    # If no sort interval specified, use the existing sort interval if there is only one
    if sort_interval_name is None:
        sort_interval_name = (SortInterval & {'nwb_file_name' : nwb_file_name})    

    # Create a spike sorting recording key
    ssr_key = {'nwb_file_name' : nwb_file_name,
               'sort_group_id' : None,
               'sort_interval_name' : sort_interval_name,
               'preproc_params_name' : 'franklab_default_hippocampus',
               'team_name' : team_name}
    # Create spike sorting recording entry
    (SpikeSortingRecordingSelection & {'nwb_file_name' : nwb_file_name}).delete()
    for sort_group in sort_groups_list:
        ssr_key['sort_group_id'] = sort_group
        SpikeSortingRecordingSelection.insert1(ssr_key)
        SpikeSortingRecording.populate([ (SpikeSortingRecordingSelection & ssr_key).proj() ])

def detect_artifacts(nwb_file_name, artifact_parameters_name='none', sort_groups_list=None, sort_interval_name=None):

    

def _get_default_parameter_sets():

    # Get default preprocessing, artifact detection, and spike sorting parameters
    default_parameters_dict = {}
    default_parameters_dict['preprocessing'] = 'franklab_default_hippocampus'
    default_parameters_dict['artifact_detection'] = 'none'
    default_parameters_dict['spike_sorting'] = 'mountainsort4'