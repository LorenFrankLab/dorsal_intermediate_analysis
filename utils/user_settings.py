import os

# Path were code repo is stored
code_path = '/stelmo/rhino/code/dorsal_intermediate_analysis/src'
# Path where each rat's raw data directory is stored
data_path = '/stelmo/rhino/implantData/'
# Spyglass base .nwb file path
spyglass_path = os.environ.get('SPYGLASS_BASE_DIR')
# Path where raw .nwb files are stored
spyglass_nwb_path = os.path.join(spyglass_path, 'raw')
# Path where .nwb associated .h264 video files are stored
spyglass_video_path = os.path.join(spyglass_path, 'video')

# List of strings indicating the pattern of epoch types cycled through during each day of recording
# For alternating sleep and run, this is typically ['s', 'r']
epoch_names_list = ['r']
# List of string indicating the camera names for each of the epoch types
# For alternating sleep and run with a separate camera for each, this is typically ['0', '1']
camera_names_list = ['1']
# The user's lab team in Spyglass
team_name = 'AutoTrack'