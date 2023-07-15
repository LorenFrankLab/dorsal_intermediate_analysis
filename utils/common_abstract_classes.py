from abc import ABC, abstractmethod

class DataReader(ABC):

    def __init__(self, verbose=False, timing=False):

        # Enable verbose output and timing decorated functions
        self._verbose = verbose
        self._timing = timing
    
    @property
    def verbose(self):
        return self._verbose
    
    @verbose.setter
    def verbose(self, value):
        self._verbose = value
    
    @property
    def timing(self):
        return self._timing
    
    @timing.setter
    def timing(self, value):
        self._timing = value

class DataWriter(ABC):

    def __init__(self, overwrite=False, verbose=False, timing=False):

        # Enable overwriting existing data, verbose output, and timing decorated functions
        self._overwrite = overwrite
        self._verbose = verbose
        self._timing = timing
    
    @property
    def overwrite(self):
        return self._overwrite
    
    @overwrite.setter
    def overwrite(self, value):
        self._overwrite = value

    @property
    def verbose(self):
        return self._verbose
    
    @verbose.setter
    def verbose(self, value):
        self._verbose = value
    
    @property
    def timing(self):
        return self._timing
    
    @timing.setter
    def timing(self, value):
        self._timing = value