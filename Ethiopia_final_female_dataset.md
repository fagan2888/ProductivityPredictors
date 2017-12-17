

```python
%matplotlib inline
```

#Imports


```python
import pandas as pd
import numpy as np
import statistics
import matplotlib as mpl
import matplotlib.pyplot as plt
import os
import seaborn as sns
```

##Import visualization model


```python
import seaborn as sns
sns.set(style="whitegrid", color_codes=True)
```

##Import model family


```python
from scipy import stats, integrate
from sklearn import linear_model
from sklearn.metrics import mean_squared_error
from pandas.plotting import scatter_matrix

from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

from sklearn.cluster import KMeans
from sklearn.linear_model import Ridge
from sklearn.linear_model import Lasso
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import SGDClassifier, Perceptron
from sklearn.neighbors import KNeighborsRegressor
```

##Upload data


```python
ntab1 = pd.read_csv("C:\\Users\\ntawi\Desktop\\Ethiopia_final_female_dataset.csv")
ntab1 = pd.DataFrame(ntab1)
ntab1.head()
```

##Identify shape of data [displays rows and columns]


```python
ntab1.shape
```

##Drop all columns that have only nan [not a number]


```python
ntab1.dropna(axis=1, how='all')
ntab1.head()
```

##Filling in missing data [optional]


```python
ntab1.fillna(0).head()
```

##Run descriptives


```python
ntab1.describe()
```

##Checking for duplicate rows


```python
ntab1.duplicated().head()
```

##Run visualization to identify pariwise relationships


```python
scatter_matrix(ntab1, alpha=0.2, figsize=(18,18), diagonal='kde')
plt.show()
```


```python

```
