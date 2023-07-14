import itertools
import numpy as np
import pandas as pd
import os
import re
from rec_to_binaries import extract_trodes_rec_file
from rec_to_binaries.read_binaries import readTrodesExtractedDataFile
from rec_to_binaries.read_binaries import write_trodes_extracted_datafile 
import shutil
import xml.etree.ElementTree as ET

from .user_settings import code_path
from .file_utils import FileInfo
from .metadata_writer import MetadataWriter

def write_trodes_config_file(subject_name, dates=None):

    # Get electrode metadata
    metadata = MetadataWriter(subject_name, dates=dates)
    electrode_df = metadata._csv_metadata['electrode']
    tetrode_ids_list = electrode_df.index.tolist()
    bad_channels_list = electrode_df['Bad channels'].tolist()
    ref_tetrode_ids_list = list(map( str, electrode_df['Ref tetrode ID'].tolist() ))

    # Get first intact electrode index for each tetrode
    lfp_channels_list = [None]*len(tetrode_ids_list)
    for ndx, bad_channels in enumerate(bad_channels_list):
        bad_channels = bad_channels.split(', ') if type(bad_channels) == str else []
        # If all channels are dead, use first channel
        if len(bad_channels) == 4:
            lfp_channels_list[ndx] = '1'
        else:
            intact_channels = list(set(['0', '1', '2', '3']) - set(bad_channels))
            lfp_channels_list[ndx] = str( int(sorted(intact_channels)[0])+1 )
    
    # Read Trodes config .xml file
    if all(int(tet_id) <= 32 for tet_id in tetrode_ids_list):
        trodes_tree = read_default_trodes_conf_32()
    elif any(int(tet_id) > 32 for tet_id in tetrode_ids_list) and all(int(tet_id) <= 64 for tet_id in tetrode_ids_list):
        trodes_tree = read_default_trodes_conf_64()
    else:
        raise NotImplementedError(f"Can't determine config file for following tetrode names list: {tetrode_ids_list}")
    
    # Modify .xml tetrode elements corresponding to each tetrode found in metadata
    for tet_id, lfp_chn, ref_id in zip(tetrode_ids_list, lfp_channels_list, ref_tetrode_ids_list):
        query_txt = f".//SpikeNTrode[@id=\"{tet_id}\"]"
        tetrode_element = trodes_tree.find(query_txt)
        # Ensure that tetrode has a corresponding element in config .xml file
        assert tetrode_element, \
            f"Could not find .xml element for tetrode ID {tet_id}"
        # Insert LFP channel number, reference tetrode ID, and reference tetrode channel number
        ref_chn = lfp_channels_list[tetrode_ids_list.index(ref_id)]
        tetrode_element.attrib['LFPChan'] = lfp_chn
        tetrode_element.attrib['refNTrodeID'] = ref_id
        tetrode_element.attrib['refChan'] = ref_chn

    # Remove any .xml tetrode elements not corresponding to a tetrode
    for tetrodes_config_element in trodes_tree.findall('SpikeConfiguration'):
        for tetrode_element in tetrodes_config_element.findall('SpikeNTrode'):
            if tetrode_element.attrib['id'] not in tetrode_ids_list:
                tetrodes_config_element.remove(tetrode_element)
    
    # Write .xml file for each date
    print(f"Writing {len(tetrode_ids_list)} tetrode elements to Trodes config files")
    for ndx, date in enumerate(metadata._dates):
        xml_file_name = metadata._file_info._file_name_helper._get_file_name_prefix(date) + '.trodesconf'
        xml_full_name = os.path.join(metadata._file_info._file_name_helper._get_paths_by_file_type('raw')[ndx], xml_file_name)
        with open(xml_full_name, "wb") as f:
            print (f"{xml_full_name}")
            f.write( ET.tostring(trodes_tree.getroot(), method='xml') )

def read_default_trodes_conf_32():

    # Read default 64-tetrode trodes config file as XML
    tree = ET.parse(os.path.join(code_path, 'utils', 'default_trodes_config_32.trodesconf'))
    return tree

def read_default_trodes_conf_64():

    # Read default 64-tetrode trodes config file as XML
    tree = ET.parse(os.path.join(code_path, 'utils', 'default_trodes_config_64.trodesconf'))
    return tree

def replace_position_tracking_files(subject_name, dates=None):

    file_info = FileInfo(subject_name, dates=dates)
    # Get list of all updated position tracking and timestamps files
    new_files_path = os.path.join('/cumulus/david/Scn2a/ImplantTracking/ImplantTracking', subject_name, subject_name)
    new_files_list = [None]*len(file_info._dates)
    for ndx, date in enumerate(file_info._dates):
        new_files_list[ndx] = [os.path.join(new_files_path, file.name) for file in os.scandir(new_files_path) if file.is_file() and re.search(date, file.name)]
    new_files_list = list(itertools.chain(*new_files_list))
    
    # Replace old files with new files
    print(f"Replacing position tracking files for {subject_name}")
    for new_file in new_files_list:
        new_file_name =os.path.split(new_file)[1]
        expected_names_df = file_info.search_expected_file_names(new_file_name)
        # Ensure new file name matches an expected file name
        assert not expected_names_df.empty, \
            f"File named {new_file} doesn't match any expected file names"
        # Ensure new file name matches only one expected file name
        assert len(expected_names_df) == 1, \
            f"File named {new_file} matches more than one expected file name"
        
        # Move new file to where old file is or should be
        old_file = expected_names_df.iloc[0]['full_name']
        old_files_list = file_info.search_file_names(new_file_name)['full_name'].tolist()
        if old_file in old_files_list:
            print(f"Overwriting {old_file} with {new_file}")
            shutil.copyfile(new_file, old_file)
        else:
            print(f"Creating {old_file} from {new_file}")
            shutil.copy(new_file, old_file)

def replace_position_unix_timestamps(subject_name, dates=None):

    # Get list of dates and implant data path
    dates = FileInfo(subject_name, dates=dates)._dates
    data_path = FileInfo._data_path
    # Path where corrected position tracking and timestamps data is stored
    tracking_path = os.path.join('/cumulus/david/Scn2a/ImplantTracking/ImplantTracking', subject_name, subject_name)

    for date in dates:
        print (f"Extracting camera sync timestamps for '{subject_name}_{date}'")
        extract_trodes_rec_file(data_path,
                                subject_name,
                                dates=[date],
                                parallel_instances=4,
                                extract_analog=False,
                                extract_spikes=False,
                                extract_lfps=False,
                                extract_dio=False,
                                extract_time=True,
                                extract_mda=False,
                                overwrite=True)

        print(f"Replacing camera sync timestamps for '{subject_name}_{date}'")
        # Raw data directory
        raw_path = os.path.join(data_path, subject_name, 'raw', date)
        # Preprocessing time directory
        time_path = os.path.join(data_path, subject_name, 'preprocessing', date)
        # Get names of hardware sync files for all epochs
        hw_sync_files_list = [name for name in os.listdir(raw_path) if re.search('.*cameraHWSync', name)]
        for hw_sync_file_name in hw_sync_files_list:
            # Replace hardware sync file with updated tracking
            src = os.path.join(tracking_path, hw_sync_file_name)
            dst = os.path.join(raw_path, hw_sync_file_name)
            print(f"Replacing '{dst}' with '{src}'")
            os.remove(dst)
            shutil.copyfile(src, dst)
            
            # Get beginning substring of file names for recording epoch
            name_substring = re.split('\.1\.videoTimeStamps\.cameraHWSync', hw_sync_file_name)[0]
            # Read hardware sync file
            pos_time = readTrodesExtractedDataFile(os.path.join(raw_path, hw_sync_file_name))
            # Read extracted continous time data file
            cont_time = readTrodesExtractedDataFile(os.path.join(time_path, name_substring+'.time', name_substring+'.continuoustime.dat'))

            # Restrict position data to Trodes samples with corresponding neural data
            samples, _, pos_ind = np.intersect1d(cont_time['data']['trodestime'],
                                                 pos_time['data']['PosTimestamp'],
                                                 assume_unique=True,
                                                 return_indices=True)
            pos_time['data'] = pos_time['data'][pos_ind]

            # Convert neural time data to dataframe using Trodes sample numbers as indices
            neu_data = pd.DataFrame(cont_time['data']).set_index('trodestime')
            # Get neural timestamps using Trodes sample numbers in position data
            neu_time = (neu_data.loc[samples]['adjusted_systime'].values)
            # Overwrite position timestamps with neural timestamps
            pos_time['data']['HWTimestamp'] = neu_time
            # Overwrite cameraHWSync file with updated position timestamps
            write_trodes_extracted_datafile(os.path.join(raw_path, hw_sync_file_name), pos_time)

def rename_nwb_file(old_name, new_name):

    # Rename .nwb file from default name
    os.rename(old_name, new_name)