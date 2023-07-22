from getpass import getpass
import os
import subprocess

from utils import verbose_printer

def create_symlink(src_full_name, dst_full_name, overwrite=False):

    # Ensure source file exists
    src_path = os.path.split(src_full_name)[0]
    if src_full_name not in [file.path for file in os.scandir(src_path) if file.is_file()]:
        raise ValueError(f"Source file {src_full_name} doesn't exist")
    
    # Either delete and overwrite or skip creating symlink if it already exists
    dst_path = os.path.split(dst_full_name)[0]
    if dst_full_name in [file.path for file in os.scandir(dst_path) if file.is_file()]:
        if not overwrite:
            # Don't delete symlink if it already exists
            no_overwrite_handler_file(f"File '{dst_full_name}' already exists")
            return
        else:
            # Delete existing symlink
            rm_cmd = 'rm ' + dst_full_name
            _subprocess_command(rm_cmd)
    
    # Create the symlink
    sym_cmd = 'ln -s ' + src_full_name + ' ' + dst_full_name
    _subprocess_command(sym_cmd)
    # Ensure that the symlink exists
    assert dst_full_name in [file.path for file in os.scandir(dst_path) if file.is_file()], \
        f"Could not find symlink {dst_full_name}"

def create_symlink_in_directory(src_full_name, dst_path, overwrite=False):
    
    # Ensure source and destination paths aren't the same
    src_path = os.path.split(src_full_name)
    if src_path == dst_path:
        raise ValueError(f"Destination path {dst_path} matches source file {src_full_name}")
    
    # Get full file name of destination file
    src_file_name = os.path.split(src_full_name)[1]
    dst_full_name = os.path.join(dst_path, src_file_name)
    # Create symlink
    create_symlink(src_full_name, dst_full_name, overwrite=overwrite)


def no_overwrite_handler_file(message):
    
    # Print message if file already exists
    verbose_printer.print_exclusion(message)

def no_overwrite_handler_table(message):
    
    # Print message if table entry already exists
    verbose_printer.print_exclusion(message)


def universal_file_permissions(full_name, pwd=None):
        
    # Make file readable, writable, and executable by anyone
    cmd = 'chmod a+rwx ' + full_name
    _subprocess_sudo_command(cmd, pwd=pwd)

def restrict_file_permissions(full_name, pwd=None):

    # Make file readable, writable, and executable only by user
    cmd = 'chmod u+rwx ' + full_name + ' && chmod go-rwx ' + full_name
    _subprocess_sudo_command(cmd, pwd=pwd)

def universal_read_only_file(full_name, pwd=None):

    # Make file read-only by anyone
    cmd = 'chmod a+rx ' + full_name + ' && chmod a-w ' + full_name
    _subprocess_sudo_command(cmd, pwd=pwd)

def restrict_read_only_file(full_name, pwd=None):

    # Make file read-only only by user
    cmd = 'chmod u+r ' + full_name + ' && chmod go-rwx ' + full_name + ' && chmod u-w ' + full_name
    _subprocess_sudo_command(cmd, pwd=pwd)


def universal_directory_permissions(path_name, pwd=None):

    # Make directory readable, writable, and executable by anyone
    cmd = 'chmod a+rwx -R ' + path_name
    _subprocess_sudo_command(cmd, pwd=pwd)

def restrict_directory_permissions(path_name, pwd=None):

    # Make directory readable, writable, and executable only by user
    cmd = 'chmod u+rwx -R ' + path_name + ' && chmod go-rwx -R ' + path_name
    _subprocess_sudo_command(cmd, pwd=pwd)

def universal_read_only_directory(path_name, pwd=None):

    # Make file read-only by anyone
    cmd = 'chmod a+rx -R ' + path_name + ' && chmod a-w -R ' + path_name
    _subprocess_sudo_command(cmd, pwd=pwd)

def restrict_read_only_directory(path_name, pwd=None):

    # Make file read-only only by user
    cmd = 'chmod u+r -R ' + path_name + ' && chmod go-rwx -R ' + path_name + ' && chmod u-w -R ' + path_name
    _subprocess_sudo_command(cmd, pwd=pwd)


def _subprocess_command(cmd):

    verbose_printer.print_text(cmd)
    # Run command with output to terminal
    subprocess.run(cmd, stdout=subprocess.PIPE, shell=True)

def _subprocess_sudo_command(cmd, pwd=None):

    # Get sudo password
    if pwd is None:
        pwd = _prompt_unix_password()
    # Set output to terminal to avoid printing sudo password to Jupyter notebook
    sudo_cmd = ['echo', pwd, '|', 'sudo', '-S']
    sudo_cmd.append(cmd)
    verbose_printer.print_text(cmd)
    subprocess.run(' '.join(sudo_cmd), stdout=subprocess.PIPE, shell=True)

def _prompt_unix_password():

    pwd = getpass('sudo permissions required, input password:')
    return pwd