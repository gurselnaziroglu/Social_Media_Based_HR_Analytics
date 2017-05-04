import re
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pylab
from datetime import datetime,timedelta
from database import database
dat = database()


############# get graphs
# dat.tNOC()
# dat.tNOU()
dat.tNOCPU()
# dat.aNOCPU()
# dat.sDOCPU()
# dat.tNOCPP()
# dat.nOCPED()
# dat.nOCPH()
# dat.nODUPP()
# dat.nODUCPH()

# ts = pd.Series(np.random.randn(1000), index=pd.date_range('1/1/2000', periods=1000))
# ts = ts.cumsum()
# print(ts)
# ts.plot()
plt.show()
dat.close()