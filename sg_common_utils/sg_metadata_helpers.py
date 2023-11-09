from .sg_helpers import (fetch,
                         query_table)
from .sg_tables import (Raw,
                        LFP)

def get_ephys_sampling_rate(nwb_file_name):

    # Get sampling rate for ephys for given .nwb file
    query = query_table(Raw, 'nwb_file_name', nwb_file_name)
    sampling_rate = fetch(query, 'sampling_rate')
    return sampling_rate[0] if sampling_rate else None

def get_lfp_sampling_rate(nwb_file_name):

    # Get sampling rate for LFP filtered ephys for given .nwb file
    query = query_table(LFP, ['nwb_file_name', 'filter_name'], [nwb_file_name, 'LFP 0-400 Hz'])
    sampling_rate = fetch(query, 'lfp_sampling_rate')
    return sampling_rate[0] if sampling_rate else None