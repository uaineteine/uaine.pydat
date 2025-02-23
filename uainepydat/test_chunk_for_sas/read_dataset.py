import pandas as pd
import pyreadstat
from uainepydat import dataio

src_data = "dataset.sas7bdat"

df = dataio.read_flat_df(src_data)