from spyglass.common import (Electrode,
                             ElectrodeGroup,
                             IntervalList,
                             Nwbfile,
                             Raw,
                             Session,
                             Subject,
                             TaskEpoch)

from spyglass.lfp import (LFPElectrodeGroup,
                          LFPOutput as LFPMerge)
from spyglass.lfp.v1 import (LFPSelection,
                             LFPV1 as LFP)
from spyglass.lfp.analysis.v1.lfp_band import (LFPBandSelection,
                                               LFPBandV1 as LFPBand)
LFPOutput = LFPMerge.LFPV1