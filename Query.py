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
# dat.tNOP()
# dat.tNOCPU()  #plot
# dat.aNOCPU()
# dat.aNOCPP()
# dat.sDOCPU()
# dat.tNOCPP()  #plot
# dat.nOCPED()  #plot
# dat.nOCPH()   #plot
# dat.nODUPP()  #plot
# dat.nODUCPH()  #plot

plt.show()
dat.close()