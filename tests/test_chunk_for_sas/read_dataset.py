import pandas as pd
import pyreadstat
from uainepydat import dataio
from uainepydat import fileio

src_data = "dataset.sas7bdat"

class chunk_reader_sas:
    def __init__(self, filepath, chunksize=1000, encoding=None, allcols=True, targetCols=["all"]):
        self.filepath = filepath
        self.encoding = encoding
        self.chunksize = chunksize
        self.useallcols = allcols
        self.targetCols = targetCols

        #read spot - start at 0
        self.chunksread = 0

        #is finished
        self.complete = False
    
    def read_chunk(self):
        if self.useallcols == False:
            #calculate row limit and start
            row_start=self.chunksize * self.chunksread
            df, meta = pyreadstat.read_sas7bdat(self.filepath, row_offset = row_start, encoding=self.encoding, usecols=self.targetCols)
        else:
            df, meta = pyreadstat.read_sas7bdat(self.filepath, row_offset = row_start, encoding=self.encoding)
        self.chunksread +=1

        #if less rows than chunksize it must have been the last
        if (df.shape[0] < self.chunksize):
            self.complete = True

        return df

df = dataio.read_flat_df(src_data)
mem = dataio.df_memory_usage(df)/(1024*1024)
