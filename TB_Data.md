```python
from google.cloud.bigquery.client import Client
from google.cloud import bigquery
from google.cloud import storage
from pathlib import Path
import numpy as np
import pandas as pd
import json
import os
import re
```


```python

HOME = os.environ['HOME']+'/'
dsops = HOME+'Documens/DevOps_DSOps/'
tbleau = HOME+'Documents/Notes/Open_Datasets/Tableau/'

```


```python
tb_df = pd.read_csv('../Notes/Open_Datasets/Tableau/TB_Burden_Country.csv')
```


```python
tb_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Country or territory name</th>
      <th>ISO 2-character country/territory code</th>
      <th>ISO 3-character country/territory code</th>
      <th>ISO numeric country/territory code</th>
      <th>Region</th>
      <th>Year</th>
      <th>Estimated total population number</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population, low bound</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population, high bound</th>
      <th>...</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive</th>
      <th>Estimated incidence of TB cases who are HIV-positive, low bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive, high bound</th>
      <th>Method to derive TBHIV estimates</th>
      <th>Case detection rate (all forms), percent</th>
      <th>Case detection rate (all forms), percent, low bound</th>
      <th>Case detection rate (all forms), percent, high bound</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1990</td>
      <td>11731193</td>
      <td>306.0</td>
      <td>156.0</td>
      <td>506.0</td>
      <td>...</td>
      <td>0.11</td>
      <td>0.08</td>
      <td>0.14</td>
      <td>12.0</td>
      <td>9.4</td>
      <td>16.0</td>
      <td>NaN</td>
      <td>20.0</td>
      <td>15.0</td>
      <td>24.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1991</td>
      <td>12612043</td>
      <td>343.0</td>
      <td>178.0</td>
      <td>562.0</td>
      <td>...</td>
      <td>0.13</td>
      <td>0.11</td>
      <td>0.16</td>
      <td>17.0</td>
      <td>14.0</td>
      <td>20.0</td>
      <td>NaN</td>
      <td>96.0</td>
      <td>80.0</td>
      <td>110.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1992</td>
      <td>13811876</td>
      <td>371.0</td>
      <td>189.0</td>
      <td>614.0</td>
      <td>...</td>
      <td>0.16</td>
      <td>0.14</td>
      <td>0.18</td>
      <td>22.0</td>
      <td>19.0</td>
      <td>24.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1993</td>
      <td>15175325</td>
      <td>392.0</td>
      <td>194.0</td>
      <td>657.0</td>
      <td>...</td>
      <td>0.19</td>
      <td>0.17</td>
      <td>0.21</td>
      <td>28.0</td>
      <td>25.0</td>
      <td>31.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1994</td>
      <td>16485018</td>
      <td>410.0</td>
      <td>198.0</td>
      <td>697.0</td>
      <td>...</td>
      <td>0.21</td>
      <td>0.18</td>
      <td>0.24</td>
      <td>35.0</td>
      <td>30.0</td>
      <td>39.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 47 columns</p>
</div>




```python
tb_df.shape
```




    (5120, 47)




```python
tb_df.columns
```




    Index(['Country or territory name', 'ISO 2-character country/territory code',
           'ISO 3-character country/territory code',
           'ISO numeric country/territory code', 'Region', 'Year',
           'Estimated total population number',
           'Estimated prevalence of TB (all forms) per 100 000 population',
           'Estimated prevalence of TB (all forms) per 100 000 population, low bound',
           'Estimated prevalence of TB (all forms) per 100 000 population, high bound',
           'Estimated prevalence of TB (all forms)',
           'Estimated prevalence of TB (all forms), low bound',
           'Estimated prevalence of TB (all forms), high bound',
           'Method to derive prevalence estimates',
           'Estimated mortality of TB cases (all forms, excluding HIV) per 100 000 population',
           'Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, low bound',
           'Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, high bound',
           'Estimated number of deaths from TB (all forms, excluding HIV)',
           'Estimated number of deaths from TB (all forms, excluding HIV), low bound',
           'Estimated number of deaths from TB (all forms, excluding HIV), high bound',
           'Estimated mortality of TB cases who are HIV-positive, per 100 000 population',
           'Estimated mortality of TB cases who are HIV-positive, per 100 000 population, low bound',
           'Estimated mortality of TB cases who are HIV-positive, per 100 000 population, high bound',
           'Estimated number of deaths from TB in people who are HIV-positive',
           'Estimated number of deaths from TB in people who are HIV-positive, low bound',
           'Estimated number of deaths from TB in people who are HIV-positive, high bound',
           'Method to derive mortality estimates',
           'Estimated incidence (all forms) per 100 000 population',
           'Estimated incidence (all forms) per 100 000 population, low bound',
           'Estimated incidence (all forms) per 100 000 population, high bound',
           'Estimated number of incident cases (all forms)',
           'Estimated number of incident cases (all forms), low bound',
           'Estimated number of incident cases (all forms), high bound',
           'Method to derive incidence estimates',
           'Estimated HIV in incident TB (percent)',
           'Estimated HIV in incident TB (percent), low bound',
           'Estimated HIV in incident TB (percent), high bound',
           'Estimated incidence of TB cases who are HIV-positive per 100 000 population',
           'Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound',
           'Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound',
           'Estimated incidence of TB cases who are HIV-positive',
           'Estimated incidence of TB cases who are HIV-positive, low bound',
           'Estimated incidence of TB cases who are HIV-positive, high bound',
           'Method to derive TBHIV estimates',
           'Case detection rate (all forms), percent',
           'Case detection rate (all forms), percent, low bound',
           'Case detection rate (all forms), percent, high bound'],
          dtype='object')




```python
tb_df['Estimated incidence of TB cases who are HIV-positive per 100 000 population']
```




    0         0.11
    1         0.13
    2         0.16
    3         0.19
    4         0.21
             ...  
    5115    511.00
    5116    487.00
    5117    448.00
    5118    411.00
    5119    395.00
    Name: Estimated incidence of TB cases who are HIV-positive per 100 000 population, Length: 5120, dtype: float64




```python
def scaling_median_risk(x):
    score = round(800 + (75*np.log(2))*np.log(x/(1-x)))
    if score < 1:
        return 1
    elif score>1000:
        return 1000
    else:
        return score

def scaling_low_risk(x):
    score = round(600 + (60*np.log(2))*np.log(x/(1-x)))
    if score < 1:
        return 1
    elif score > 1000:
        return 1000
    else:
        return score
    
def scaling_high_risk(x):
    score = round(600 + (52*np.log(2))*np.log(x/(1-x)))
    if score < 1:
        return 1
    elif score > 1000:
        return 1000
    else:
        return score
        

```


```python
tb_df.head(24)['Estimated incidence of TB cases who are HIV-positive per 100 000 population'].apply(scaling_median_risk)


```


```python
tb_df.head(24)['Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound'].apply(scaling_low_risk)

```




    0     498
    1     513
    2     525
    3     534
    4     537
    5     545
    6     550
    7     556
    8     559
    9     563
    10    567
    11    571
    12    574
    13    578
    14    581
    15    587
    16    590
    17    595
    18    600
    19    603
    20    605
    21    608
    22    610
    23    612
    Name: Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound, dtype: int64




```python
tb_df.head(24)['Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound'].apply(scaling_high_risk)

```




    0     535
    1     540
    2     545
    3     552
    4     558
    5     564
    6     566
    7     569
    8     573
    9     578
    10    579
    11    582
    12    585
    13    590
    14    594
    15    599
    16    601
    17    606
    18    610
    19    616
    20    621
    21    624
    22    629
    23    634
    Name: Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound, dtype: int64




```python
tb_df['Estimated incidence of TB cases who are HIV-positive per 100 000 population']
```




    0         0.11
    1         0.13
    2         0.16
    3         0.19
    4         0.21
             ...  
    5115    511.00
    5116    487.00
    5117    448.00
    5118    411.00
    5119    395.00
    Name: Estimated incidence of TB cases who are HIV-positive per 100 000 population, Length: 5120, dtype: float64




```python
x = np.arange(1, 12, 1)
```


```python
x
```




    array([ 1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11])




```python
y = tb_df.head(24)['Estimated incidence of TB cases who are HIV-positive per 100 000 population'].to_list()
```


```python
def func2(t, a, b):
    return a * pow(b, t)
```


```python
from scipy.optimize import curve_fit
```


```python
[a, b], res = curve_fit(func2, x, y)
```


```python

```


```python

```


```python
tb_df['Estimated incidence of TB cases who are HIV-positive per 100 000 population'][0]
```




    np.float64(0.11)




```python

```


```python

```


```python
import json
from pathlib import Path
import pandas as pd

# tb_df is assumed to already exist

def pandas_dtype_to_bq_type(dtype: pd.api.types.CategoricalDtype | str) -> str:
    """
    Map pandas dtype -> BigQuery type.
    Adjust mappings if you have specific needs (e.g., NUMERIC vs FLOAT).
    """
    dt = str(dtype).lower()

    if dt in ("object", "string") or dt.startswith("string"):
        return "STRING"
    if dt.startswith("int") or dt.startswith("uint"):
        return "INT64"
    if dt.startswith("float"):
        return "FLOAT64"
    if dt == "bool" or dt == "boolean":
        return "BOOL"
    if dt.startswith("datetime64"):
        return "TIMESTAMP"  # use "DATETIME" if you prefer no timezone semantics
    if dt.startswith("date"):
        return "DATE"
    if dt.startswith("timedelta64"):
        return "TIME"
    if dt == "category":
        return "STRING"
    # Fallback
    return "STRING"


def write_bq_schema_json(df: pd.DataFrame, out_path: str | Path) -> list[dict]:
    """
    Create a BigQuery schema JSON file for bq load, inferred from df.dtypes.
    Output format matches BigQuery's JSON schema: [{"name":..., "type":..., "mode":"NULLABLE"}, ...]
    """
    schema = []
    for col, dtype in df.dtypes.items():
        schema.append({
            "name": col,
            "type": pandas_dtype_to_bq_type(dtype),
            "mode": "NULLABLE",   # change to "REQUIRED" if you enforce non-nulls
        })

    out_path = Path(out_path)
    out_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
    return schema


# --- Create schema file ---
schema = write_bq_schema_json(tb_df, tbleau+"tb_schema.json")

# Optional: print a quick preview
print(f"Wrote {len(schema)} fields to tb_schema.json")
print(schema[:5])
```

    Wrote 47 fields to tb_schema.json
    [{'name': 'Country or territory name', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'ISO 2-character country/territory code', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'ISO 3-character country/territory code', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'ISO numeric country/territory code', 'type': 'INT64', 'mode': 'NULLABLE'}, {'name': 'Region', 'type': 'STRING', 'mode': 'NULLABLE'}]



```python

```


```python

```


```python
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch import optim
from torch.utils.data import DataLoader
from torch.utils.data import Dataset
from sklearn.preprocessing import StandardScaler
from sklearn.utils import resample
import warnings
from datetime import datetime,timedelta


```


```python

#days_back = getArgument("set_timestamp","PARAMS")
days_back = 1
db = days_back
warnings.filterwarnings('ignore')

    
time_form = datetime.now() - timedelta(db)
timestamp = str((time_form).strftime('%Y-%m-%d'))

print(timestamp)
datetime.now().strftime('%H:%M:%S')


```


```python



class moving_avg(nn.Module):
    """
    Moving average block to highlight the trend of time series
    """
    def __init__(self, kernel_size, stride, pad=True):
        super(moving_avg, self).__init__()
        self.kernel_size = kernel_size
        self.avg = nn.AvgPool1d(kernel_size=kernel_size, stride=stride, padding=0)
        self.pad = pad

    def forward(self, x):
        # padding on the both ends of time series
        if self.pad:
            front = x[:, 0:1, :].repeat(1, (self.kernel_size - 1) // 2, 1)
            end = x[:, -1:, :].repeat(1, (self.kernel_size - 1) // 2, 1)
            x = torch.cat([front, x, end], dim=1)
        x = self.avg(x.permute(0, 2, 1))
        x = x.permute(0, 2, 1)
        return x


class series_decomp(nn.Module):
    """
    Series decomposition block
    """
    def __init__(self, kernel_size, stride):
        super(series_decomp, self).__init__()
        self.moving_avg = moving_avg(kernel_size, stride=stride)

    def forward(self, x):
        moving_mean = self.moving_avg(x)
        res = x - moving_mean
        return res, moving_mean

class Model(nn.Module):
    """
    Decomposition-Linear
    """
    def __init__(self, configs):
        super(Model, self).__init__()
        self.seq_len = configs['seq_len']
        self.pred_len = configs['pred_len']

        # Decompsition Kernel Size
        kernel_size = 3
        stride = 1
        self.decompsition = series_decomp(kernel_size, stride)
        self.individual = configs['individual']
        self.channels = configs['enc_in']

        if self.individual:
            self.Linear_Seasonal = nn.ModuleList()
            self.Linear_Trend = nn.ModuleList()
            
            for i in range(self.channels):
                self.Linear_Seasonal.append(nn.Linear(self.seq_len,self.pred_len))
                self.Linear_Trend.append(nn.Linear(self.seq_len,self.pred_len))

                # Use this two lines if you want to visualize the weights
                # self.Linear_Seasonal[i].weight = nn.Parameter((1/self.seq_len)*torch.ones([self.pred_len,self.seq_len]))
                # self.Linear_Trend[i].weight = nn.Parameter((1/self.seq_len)*torch.ones([self.pred_len,self.seq_len]))
        else:
            self.Linear_Seasonal = nn.Linear(self.seq_len,self.pred_len)
            self.Linear_Trend = nn.Linear(self.seq_len,self.pred_len)
            
            # Use this two lines if you want to visualize the weights
            # self.Linear_Seasonal.weight = nn.Parameter((1/self.seq_len)*torch.ones([self.pred_len,self.seq_len]))
            # self.Linear_Trend.weight = nn.Parameter((1/self.seq_len)*torch.ones([self.pred_len,self.seq_len]))
        self.activation = nn.ReLU()
        self.final_layer = nn.Linear(self.channels,self.pred_len)
        self.final_activation = nn.Sigmoid()

    def forward(self, x):
        # x: [Batch, Input length, Channel]
        
        seasonal_init, trend_init = self.decompsition(x)
        
        seasonal_init, trend_init = seasonal_init.permute(0,2,1), trend_init.permute(0,2,1)
        if self.individual:
            seasonal_output = torch.zeros([seasonal_init.size(0),seasonal_init.size(1),self.pred_len],dtype=seasonal_init.dtype).to(seasonal_init.device)
            trend_output = torch.zeros([trend_init.size(0),trend_init.size(1),self.pred_len],dtype=trend_init.dtype).to(trend_init.device)
            for i in range(self.channels):
                seasonal_output[:,i,:] = self.Linear_Seasonal[i](seasonal_init[:,i,:])
                trend_output[:,i,:] = self.Linear_Trend[i](trend_init[:,i,:])
        else:
            seasonal_output = self.Linear_Seasonal(seasonal_init)
            trend_output = self.Linear_Trend(trend_init)

        x = seasonal_output + trend_output
        # x = self.activation(x)
        x = self.final_layer(x.view(-1,self.channels))
        x = self.final_activation(x)
        return x.view(-1) # to [Batch, Output length, Channel]


class Time_series_dataset(Dataset):
    
    def __init__(self,
                 data = pd.DataFrame(),
                 seq_len = 90,
                 feats = ['total_items_sold'],
                 scale=True):
        
        self.seq_len = seq_len
        self.feats = feats # column names to include
        self.scale = scale # normalize data or not
        self.scaler = StandardScaler()
        if not data.empty:
            self._prepare_tensor_data_(data)
        else:
            raise Exception("Dataframe is empty")
            
    def _prepare_tensor_data_(self, df_raw):
        df_raw.reset_index(drop=True,inplace=True)

        if self.feats:
            self.data_X = df_raw[self.feats]
        else:
            self.data_X = df_raw[:,-1] # incase if feats not specified, take last col as test data 

        

        #scaling logic
        if self.scale:
            self.data_X = self.data_X.apply(lambda x: self.__scale_feats__(x), axis=1)
        else:
            self.data_X = self.data_X.apply(lambda x: self.__unscaled_feats__(x), axis=1)
            
        print(self.data_X.shape, self.data_X[0].shape)

        del df_raw
        
    
    def __scale_feats__(self, row):
        # print(row.shape)
        row = [[0.0]*(self.seq_len-len(v))+v for v in row.values] #pad
        row = [v[-self.seq_len:] for v in row]
#         print(row)
        x = [self.scaler.fit_transform(np.array(v).reshape(-1,1)).ravel() for v in row]
#         x = x[::-1]
        return np.array(x)
    
    def __unscaled_feats__(self, row):
        row = [[0.0]*(self.seq_len-len(v))+v for v in row.values]
        row = [v[-self.seq_len:] for v in row]
        x = [np.array(v) for v in row]
        return np.array(x)


    def __getitem__(self,index):
        #return a batch of data
        # begin = index
        # end = index + self.batch_size if index + self.batch_size < len(self.data_X) else len(self.data_X)
        rdf_feats = np.dstack(self.data_X[index])
        
        return rdf_feats


    def __len__(self):
        return len(self.data_X)
    


```


```python

print('dat length\n{}'.format(len(tb_df)))

print(tb_df.columns)
tb_df.head(4)
```

    dat length
    4636
    Index(['Country or territory name', 'ISO 2-character country/territory code',
           'ISO 3-character country/territory code',
           'ISO numeric country/territory code', 'Region', 'Year',
           'Estimated total population number',
           'Estimated prevalence of TB (all forms) per 100 000 population',
           'Estimated prevalence of TB (all forms) per 100 000 population, low bound',
           'Estimated prevalence of TB (all forms) per 100 000 population, high bound',
           'Estimated prevalence of TB (all forms)',
           'Estimated prevalence of TB (all forms), low bound',
           'Estimated prevalence of TB (all forms), high bound',
           'Method to derive prevalence estimates',
           'Estimated mortality of TB cases (all forms, excluding HIV) per 100 000 population',
           'Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, low bound',
           'Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, high bound',
           'Estimated number of deaths from TB (all forms, excluding HIV)',
           'Estimated number of deaths from TB (all forms, excluding HIV), low bound',
           'Estimated number of deaths from TB (all forms, excluding HIV), high bound',
           'Estimated mortality of TB cases who are HIV-positive, per 100 000 population',
           'Estimated mortality of TB cases who are HIV-positive, per 100 000 population, low bound',
           'Estimated mortality of TB cases who are HIV-positive, per 100 000 population, high bound',
           'Estimated number of deaths from TB in people who are HIV-positive',
           'Estimated number of deaths from TB in people who are HIV-positive, low bound',
           'Estimated number of deaths from TB in people who are HIV-positive, high bound',
           'Method to derive mortality estimates',
           'Estimated incidence (all forms) per 100 000 population',
           'Estimated incidence (all forms) per 100 000 population, low bound',
           'Estimated incidence (all forms) per 100 000 population, high bound',
           'Estimated number of incident cases (all forms)',
           'Estimated number of incident cases (all forms), low bound',
           'Estimated number of incident cases (all forms), high bound',
           'Method to derive incidence estimates',
           'Estimated HIV in incident TB (percent)',
           'Estimated HIV in incident TB (percent), low bound',
           'Estimated HIV in incident TB (percent), high bound',
           'Estimated incidence of TB cases who are HIV-positive per 100 000 population',
           'Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound',
           'Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound',
           'Estimated incidence of TB cases who are HIV-positive',
           'Estimated incidence of TB cases who are HIV-positive, low bound',
           'Estimated incidence of TB cases who are HIV-positive, high bound',
           'Method to derive TBHIV estimates',
           'Case detection rate (all forms), percent',
           'Case detection rate (all forms), percent, low bound',
           'Case detection rate (all forms), percent, high bound'],
          dtype='str')





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Country or territory name</th>
      <th>ISO 2-character country/territory code</th>
      <th>ISO 3-character country/territory code</th>
      <th>ISO numeric country/territory code</th>
      <th>Region</th>
      <th>Year</th>
      <th>Estimated total population number</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population, low bound</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population, high bound</th>
      <th>...</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive</th>
      <th>Estimated incidence of TB cases who are HIV-positive, low bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive, high bound</th>
      <th>Method to derive TBHIV estimates</th>
      <th>Case detection rate (all forms), percent</th>
      <th>Case detection rate (all forms), percent, low bound</th>
      <th>Case detection rate (all forms), percent, high bound</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1990</td>
      <td>11731193</td>
      <td>306.0</td>
      <td>156.0</td>
      <td>506.0</td>
      <td>...</td>
      <td>0.11</td>
      <td>0.08</td>
      <td>0.14</td>
      <td>12.0</td>
      <td>9.4</td>
      <td>16.0</td>
      <td>NaN</td>
      <td>20.0</td>
      <td>15.0</td>
      <td>24.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1991</td>
      <td>12612043</td>
      <td>343.0</td>
      <td>178.0</td>
      <td>562.0</td>
      <td>...</td>
      <td>0.13</td>
      <td>0.11</td>
      <td>0.16</td>
      <td>17.0</td>
      <td>14.0</td>
      <td>20.0</td>
      <td>NaN</td>
      <td>96.0</td>
      <td>80.0</td>
      <td>110.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1992</td>
      <td>13811876</td>
      <td>371.0</td>
      <td>189.0</td>
      <td>614.0</td>
      <td>...</td>
      <td>0.16</td>
      <td>0.14</td>
      <td>0.18</td>
      <td>22.0</td>
      <td>19.0</td>
      <td>24.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1993</td>
      <td>15175325</td>
      <td>392.0</td>
      <td>194.0</td>
      <td>657.0</td>
      <td>...</td>
      <td>0.19</td>
      <td>0.17</td>
      <td>0.21</td>
      <td>28.0</td>
      <td>25.0</td>
      <td>31.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>4 rows × 47 columns</p>
</div>




```python

def aggregate_df(rdf):
    df = rdf.groupby(['ISO 3-character country/territory code'], as_index=True).agg({
        'Estimated total population number':lambda x: list(x),
        'Estimated prevalence of TB (all forms) per 100 000 population':lambda x: list(x),
        'Estimated prevalence of TB (all forms) per 100 000 population, low bound':lambda x: list(x),
        'Estimated prevalence of TB (all forms) per 100 000 population, high bound':lambda x: list(x),
        'Estimated prevalence of TB (all forms)':lambda x: list(x),
        'Estimated prevalence of TB (all forms), low bound':lambda x: list(x),
        'Estimated prevalence of TB (all forms), high bound':lambda x: list(x),
        'Estimated mortality of TB cases (all forms, excluding HIV) per 100 000 population':lambda x: list(x),
        'Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, low bound':lambda x: list(x),
        'Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, high bound':lambda x: list(x),
        'Estimated number of deaths from TB (all forms, excluding HIV)':lambda x: list(x),
        'Estimated number of deaths from TB (all forms, excluding HIV), low bound':lambda x: list(x),
        'Estimated number of deaths from TB (all forms, excluding HIV), high bound':lambda x: list(x),
        'Estimated mortality of TB cases who are HIV-positive, per 100 000 population':lambda x: list(x),
        'Estimated mortality of TB cases who are HIV-positive, per 100 000 population, low bound':lambda x: list(x),
        'Estimated mortality of TB cases who are HIV-positive, per 100 000 population, high bound':lambda x: list(x),
        'Estimated number of deaths from TB in people who are HIV-positive':lambda x: list(x),
        'Estimated number of deaths from TB in people who are HIV-positive, low bound':lambda x: list(x),
        'Estimated number of deaths from TB in people who are HIV-positive, high bound':lambda x: list(x),
        'Estimated incidence (all forms) per 100 000 population':lambda x: list(x),
        'Estimated incidence (all forms) per 100 000 population, low bound':lambda x: list(x),
        'Estimated incidence (all forms) per 100 000 population, high bound':lambda x: list(x),
        'Estimated number of incident cases (all forms)':lambda x: list(x),
        'Estimated number of incident cases (all forms), low bound':lambda x: list(x),
        'Estimated number of incident cases (all forms), high bound':lambda x: list(x),
        'Estimated HIV in incident TB (percent)':lambda x: list(x),
        'Estimated HIV in incident TB (percent), low bound':lambda x: list(x),
        'Estimated HIV in incident TB (percent), high bound':lambda x: list(x),
        'Estimated incidence of TB cases who are HIV-positive per 100 000 population':lambda x: list(x),
        'Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound':lambda x: list(x),
        'Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound':lambda x: list(x),
        'Estimated incidence of TB cases who are HIV-positive':lambda x: list(x),
        'Estimated incidence of TB cases who are HIV-positive, low bound':lambda x: list(x),
        'Estimated incidence of TB cases who are HIV-positive, high bound':lambda x: list(x),
        'Case detection rate (all forms), percent':lambda x: list(x),
        'Case detection rate (all forms), percent, low bound':lambda x: list(x),
        'Case detection rate (all forms), percent, high bound':lambda x: list(x)
    })
#     print(df.info())
    return df


# %%timeit
agg_df = aggregate_df(tb_df)


print('add_df length\n{}'.format(len(agg_df)))


```

    add_df length
    200



```python
agg_df.head(4)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Estimated total population number</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population, low bound</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population, high bound</th>
      <th>Estimated prevalence of TB (all forms)</th>
      <th>Estimated prevalence of TB (all forms), low bound</th>
      <th>Estimated prevalence of TB (all forms), high bound</th>
      <th>Estimated mortality of TB cases (all forms, excluding HIV) per 100 000 population</th>
      <th>Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, low bound</th>
      <th>Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, high bound</th>
      <th>...</th>
      <th>Estimated HIV in incident TB (percent), high bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive</th>
      <th>Estimated incidence of TB cases who are HIV-positive, low bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive, high bound</th>
      <th>Case detection rate (all forms), percent</th>
      <th>Case detection rate (all forms), percent, low bound</th>
      <th>Case detection rate (all forms), percent, high bound</th>
    </tr>
    <tr>
      <th>ISO 3-character country/territory code</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>AFG</th>
      <td>[11731193, 12612043, 13811876, 15175325, 16485...</td>
      <td>[306.0, 343.0, 371.0, 392.0, 410.0, 424.0, 438...</td>
      <td>[156.0, 178.0, 189.0, 194.0, 198.0, 199.0, 202...</td>
      <td>[506.0, 562.0, 614.0, 657.0, 697.0, 733.0, 764...</td>
      <td>[36000.0, 43000.0, 51000.0, 59000.0, 68000.0, ...</td>
      <td>[18000.0, 22000.0, 26000.0, 30000.0, 33000.0, ...</td>
      <td>[59000.0, 71000.0, 85000.0, 100000.0, 110000.0...</td>
      <td>[37.0, 46.0, 54.0, 60.0, 65.0, 69.0, 71.0, 72....</td>
      <td>[24.0, 29.0, 34.0, 38.0, 41.0, 45.0, 48.0, 51....</td>
      <td>[54.0, 61.0, 68.0, 73.0, 79.0, 82.0, 85.0, 85....</td>
      <td>...</td>
      <td>[0.08, 0.09, 0.1, 0.11, 0.13, 0.15, 0.16, 0.17...</td>
      <td>[0.11, 0.13, 0.16, 0.19, 0.21, 0.23, 0.25, 0.2...</td>
      <td>[0.08, 0.11, 0.14, 0.17, 0.18, 0.21, 0.23, 0.2...</td>
      <td>[0.14, 0.16, 0.18, 0.21, 0.24, 0.27, 0.28, 0.3...</td>
      <td>[12.0, 17.0, 22.0, 28.0, 35.0, 41.0, 47.0, 53....</td>
      <td>[9.4, 14.0, 19.0, 25.0, 30.0, 37.0, 42.0, 49.0...</td>
      <td>[16.0, 20.0, 24.0, 31.0, 39.0, 47.0, 52.0, 58....</td>
      <td>[20.0, 96.0, nan, nan, nan, nan, nan, 3.6, 8.3...</td>
      <td>[15.0, 80.0, nan, nan, nan, nan, nan, 3.3, 7.8...</td>
      <td>[24.0, 110.0, nan, nan, nan, nan, nan, 4.0, 9....</td>
    </tr>
    <tr>
      <th>AGO</th>
      <td>[10333844, 10652727, 11002758, 11372156, 11743...</td>
      <td>[365.0, 387.0, 406.0, 420.0, 430.0, 435.0, 436...</td>
      <td>[185.0, 198.0, 207.0, 214.0, 218.0, 221.0, 222...</td>
      <td>[606.0, 639.0, 671.0, 694.0, 712.0, 721.0, 720...</td>
      <td>[38000.0, 41000.0, 45000.0, 48000.0, 50000.0, ...</td>
      <td>[19000.0, 21000.0, 23000.0, 24000.0, 26000.0, ...</td>
      <td>[63000.0, 68000.0, 74000.0, 79000.0, 84000.0, ...</td>
      <td>[47.0, 52.0, 57.0, 60.0, 62.0, 63.0, 62.0, 59....</td>
      <td>[32.0, 36.0, 39.0, 41.0, 43.0, 44.0, 43.0, 41....</td>
      <td>[62.0, 67.0, 71.0, 73.0, 75.0, 75.0, 75.0, 73....</td>
      <td>...</td>
      <td>[1.2, 1.8, 2.5, 3.4, 4.4, 5.4, 6.4, 7.3, 7.9, ...</td>
      <td>[1.7, 2.8, 4.2, 5.9, 7.8, 10.0, 12.0, 15.0, 17...</td>
      <td>[0.93, 1.7, 2.7, 3.9, 5.6, 7.3, 9.3, 12.0, 14....</td>
      <td>[1.9, 3.1, 4.6, 6.6, 8.8, 11.0, 13.0, 16.0, 18...</td>
      <td>[180.0, 300.0, 460.0, 670.0, 920.0, 1200.0, 15...</td>
      <td>[96.0, 180.0, 290.0, 450.0, 650.0, 890.0, 1200...</td>
      <td>[200.0, 330.0, 510.0, 750.0, 1000.0, 1300.0, 1...</td>
      <td>[48.0, 50.0, 48.0, 33.0, 27.0, 19.0, 54.0, 50....</td>
      <td>[37.0, 41.0, 41.0, 30.0, 25.0, 17.0, 49.0, 46....</td>
      <td>[59.0, 58.0, 55.0, 37.0, 31.0, 21.0, 60.0, 55....</td>
    </tr>
    <tr>
      <th>ANT</th>
      <td>[188234, 190108, 192220, 194054, 194909, 19433...</td>
      <td>[6.7, 6.7, 6.8, 6.9, 7.0, 7.2, 4.5, 16.0, 5.3,...</td>
      <td>[2.7, 2.8, 2.8, 2.9, 3.0, 3.2, 1.3, 7.0, 2.0, ...</td>
      <td>[12.0, 12.0, 13.0, 13.0, 13.0, 13.0, 9.5, 28.0...</td>
      <td>[13.0, 13.0, 13.0, 13.0, 14.0, 14.0, 8.7, 30.0...</td>
      <td>[5.1, 5.2, 5.4, 5.7, 5.9, 6.2, 2.6, 13.0, 3.7,...</td>
      <td>[23.0, 24.0, 24.0, 24.0, 25.0, 25.0, 18.0, 53....</td>
      <td>[0.59, 0.57, 0.57, 0.56, 0.55, 0.57, 0.54, 0.5...</td>
      <td>[0.56, 0.55, 0.54, 0.54, 0.53, 0.54, 0.53, 0.5...</td>
      <td>[0.62, 0.59, 0.6, 0.58, 0.57, 0.6, 0.55, 0.56,...</td>
      <td>...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, 87.0, 87.0, 87....</td>
      <td>[nan, nan, nan, nan, nan, nan, 77.0, 77.0, 77....</td>
      <td>[nan, nan, nan, nan, nan, nan, 99.0, 99.0, 99....</td>
    </tr>
    <tr>
      <th>ARE</th>
      <td>[1806498, 1908002, 2012977, 2121143, 2232159, ...</td>
      <td>[22.0, 22.0, 22.0, 22.0, 22.0, 22.0, 22.0, 22....</td>
      <td>[9.9, 9.8, 9.8, 9.9, 9.9, 9.9, 9.8, 9.8, 9.8, ...</td>
      <td>[38.0, 38.0, 38.0, 38.0, 38.0, 38.0, 38.0, 38....</td>
      <td>[390.0, 410.0, 430.0, 460.0, 480.0, 510.0, 530...</td>
      <td>[180.0, 190.0, 200.0, 210.0, 220.0, 230.0, 240...</td>
      <td>[680.0, 720.0, 760.0, 800.0, 840.0, 890.0, 940...</td>
      <td>[0.69, 0.69, 0.69, 0.69, 0.69, 0.69, 0.69, 0.6...</td>
      <td>[0.54, 0.54, 0.54, 0.54, 0.54, 0.54, 0.54, 0.5...</td>
      <td>[0.85, 0.85, 0.85, 0.85, 0.85, 0.85, 0.85, 0.8...</td>
      <td>...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[nan, nan, nan, nan, nan, nan, nan, nan, nan, ...</td>
      <td>[130.0, 100.0, 94.0, nan, 160.0, nan, 170.0, n...</td>
      <td>[100.0, 78.0, 71.0, nan, 120.0, nan, 130.0, na...</td>
      <td>[180.0, 140.0, 130.0, nan, 220.0, nan, 230.0, ...</td>
    </tr>
  </tbody>
</table>
<p>4 rows × 37 columns</p>
</div>




```python

```


```python
features = ['Estimated total population number',
       'Estimated prevalence of TB (all forms) per 100 000 population',
       'Estimated prevalence of TB (all forms) per 100 000 population, low bound',
       'Estimated prevalence of TB (all forms) per 100 000 population, high bound',
       'Estimated prevalence of TB (all forms)',
       'Estimated prevalence of TB (all forms), low bound',
       'Estimated prevalence of TB (all forms), high bound',
       'Estimated mortality of TB cases (all forms, excluding HIV) per 100 000 population',
       'Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, low bound',
       'Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, high bound',
       'Estimated number of deaths from TB (all forms, excluding HIV)',
       'Estimated number of deaths from TB (all forms, excluding HIV), low bound',
       'Estimated number of deaths from TB (all forms, excluding HIV), high bound',
       'Estimated mortality of TB cases who are HIV-positive, per 100 000 population',
       'Estimated mortality of TB cases who are HIV-positive, per 100 000 population, low bound',
       'Estimated mortality of TB cases who are HIV-positive, per 100 000 population, high bound',
       'Estimated number of deaths from TB in people who are HIV-positive',
       'Estimated number of deaths from TB in people who are HIV-positive, low bound',
       'Estimated number of deaths from TB in people who are HIV-positive, high bound',
       'Estimated incidence (all forms) per 100 000 population',
       'Estimated incidence (all forms) per 100 000 population, low bound',
       'Estimated incidence (all forms) per 100 000 population, high bound',
       'Estimated number of incident cases (all forms)',
       'Estimated number of incident cases (all forms), low bound',
       'Estimated number of incident cases (all forms), high bound',
       'Estimated HIV in incident TB (percent)',
       'Estimated HIV in incident TB (percent), low bound',
       'Estimated HIV in incident TB (percent), high bound',
       'Estimated incidence of TB cases who are HIV-positive per 100 000 population',
       'Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound',
       'Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound',
       'Estimated incidence of TB cases who are HIV-positive',
       'Estimated incidence of TB cases who are HIV-positive, low bound',
       'Estimated incidence of TB cases who are HIV-positive, high bound',
       'Case detection rate (all forms), percent',
       'Case detection rate (all forms), percent, low bound',
       'Case detection rate (all forms), percent, high bound']
```


```python


device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
seq_length = 90
pred_length = 1
is_scled = True
individual_channels = True
final_layer = len(features)
run_loc = tbleau+'DLmodel_weights'



```


```python


net = Model(configs={'seq_len':seq_length,
                            'pred_len': pred_length,
                            'individual': individual_channels,
                            'enc_in': final_layer}).to(device)


```


```python

torch.save(net.state_dict(), run_loc+'/model_weights.pt')

```


```python


net.load_state_dict(torch.load(run_loc+'/model_weights.pt'))


```




    <All keys matched successfully>




```python

```


```python


dt = Time_series_dataset(
                data = agg_df,
                seq_len = 90,
                 feats = features,
                 scale=is_scled
                )

```

    (200,) (37, 90)



```python

data_stream = DataLoader(dt,shuffle=False, batch_size=128, pin_memory=True)


```


```python

probabilities = []
with torch.no_grad():
    for i, (x_feats) in enumerate(data_stream):
        #outputs generate
        x_feats = x_feats.view(-1,seq_length,len(features)).to(device)
        outputs = net(x_feats.float())
        probabilities.append(outputs)

probabilities = torch.cat(probabilities)
test_probs_cpu = probabilities.data.cpu().numpy()
agg_df['model_score'] = list(test_probs_cpu)


```


```python

print(agg_df.head())


```


```python

agg_df.sort_values(by='model_score', ascending=False, inplace=True)


```


```python

agg_df_sample = agg_df[['Estimated incidence (all forms) per 100 000 population, high bound','Case detection rate (all forms), percent','model_score']]


```


```python
agg_df_sample.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Estimated incidence (all forms) per 100 000 population, high bound</th>
      <th>Case detection rate (all forms), percent</th>
      <th>model_score</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>8</th>
      <td>[252.0, 250.0, 245.0, 240.0, 243.0, 241.0, 238...</td>
      <td>[21.0, 23.0, 23.0, 25.0, 27.0, 25.0, 25.0, 27....</td>
      <td>0.604200</td>
    </tr>
    <tr>
      <th>11</th>
      <td>[82.0, 81.0, 79.0, 76.0]</td>
      <td>[80.0, 76.0, 77.0, 76.0]</td>
      <td>0.591949</td>
    </tr>
    <tr>
      <th>82</th>
      <td>[109.0, 116.0, 129.0, 146.0, 168.0, 189.0, 207...</td>
      <td>[55.0, 54.0, 49.0, 40.0, 40.0, 44.0, 47.0, 53....</td>
      <td>0.588292</td>
    </tr>
    <tr>
      <th>101</th>
      <td>[66.0, 73.0, 82.0, 94.0, 107.0, 119.0, 129.0, ...</td>
      <td>[70.0, 67.0, 55.0, 64.0, 61.0, 62.0, 57.0, 54....</td>
      <td>0.588254</td>
    </tr>
    <tr>
      <th>80</th>
      <td>[110.0, 150.0, 198.0, 250.0, 300.0, 343.0, 372...</td>
      <td>[65.0, 48.0, 37.0, 28.0, 24.0, 23.0, 26.0, 29....</td>
      <td>0.587361</td>
    </tr>
  </tbody>
</table>
</div>




```python


print('length of Model_scores over 0.50\n{}'.format(len(agg_df_sample[agg_df_sample['model_score'] > 0.50])))
```

    length of Model_scores over 0.50
    63



```python
len(tb_df['Year'])
```




    4636




```python
import random
```


```python
date_col = tb_df['Year'].head(4)
```


```python
random.randint(0,5)
```




    0




```python
for yr in tb_df['Year'].head(4).values:
    print(yr)
    
```

    1990
    1991
    1992
    1993



```python
tb_df['Year'].head(4).values
```




    array([1990, 1991, 1992, 1993])




```python
def amend_date_fun(yr):
    mon = random.randint(1,12)
    day = random.randint(1,28)
    return datetime(yr, mon, day).strftime('%Y-%m-%d')


```


```python
date_col = date_col.apply(amend_date_fun)
```


```python
amend_date_fun(1990)
```




    '1990-06-08'




```python
date_col
```




    0    1990-11-14
    1    1991-06-18
    2    1992-09-25
    3    1993-01-21
    Name: Year, dtype: str




```python
tb_df.head(3)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Country or territory name</th>
      <th>ISO 2-character country/territory code</th>
      <th>ISO 3-character country/territory code</th>
      <th>ISO numeric country/territory code</th>
      <th>Region</th>
      <th>Year</th>
      <th>Estimated total population number</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population, low bound</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population, high bound</th>
      <th>...</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive</th>
      <th>Estimated incidence of TB cases who are HIV-positive, low bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive, high bound</th>
      <th>Method to derive TBHIV estimates</th>
      <th>Case detection rate (all forms), percent</th>
      <th>Case detection rate (all forms), percent, low bound</th>
      <th>Case detection rate (all forms), percent, high bound</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1990</td>
      <td>11731193</td>
      <td>306.0</td>
      <td>156.0</td>
      <td>506.0</td>
      <td>...</td>
      <td>0.11</td>
      <td>0.08</td>
      <td>0.14</td>
      <td>12.0</td>
      <td>9.4</td>
      <td>16.0</td>
      <td>NaN</td>
      <td>20.0</td>
      <td>15.0</td>
      <td>24.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1991</td>
      <td>12612043</td>
      <td>343.0</td>
      <td>178.0</td>
      <td>562.0</td>
      <td>...</td>
      <td>0.13</td>
      <td>0.11</td>
      <td>0.16</td>
      <td>17.0</td>
      <td>14.0</td>
      <td>20.0</td>
      <td>NaN</td>
      <td>96.0</td>
      <td>80.0</td>
      <td>110.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1992</td>
      <td>13811876</td>
      <td>371.0</td>
      <td>189.0</td>
      <td>614.0</td>
      <td>...</td>
      <td>0.16</td>
      <td>0.14</td>
      <td>0.18</td>
      <td>22.0</td>
      <td>19.0</td>
      <td>24.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>3 rows × 47 columns</p>
</div>




```python
tb_df['Year'] = tb_df['Year'].apply(amend_date_fun)
```


```python
tb_df.head(3)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Country or territory name</th>
      <th>ISO 2-character country/territory code</th>
      <th>ISO 3-character country/territory code</th>
      <th>ISO numeric country/territory code</th>
      <th>Region</th>
      <th>Year</th>
      <th>Estimated total population number</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population, low bound</th>
      <th>Estimated prevalence of TB (all forms) per 100 000 population, high bound</th>
      <th>...</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive</th>
      <th>Estimated incidence of TB cases who are HIV-positive, low bound</th>
      <th>Estimated incidence of TB cases who are HIV-positive, high bound</th>
      <th>Method to derive TBHIV estimates</th>
      <th>Case detection rate (all forms), percent</th>
      <th>Case detection rate (all forms), percent, low bound</th>
      <th>Case detection rate (all forms), percent, high bound</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1990-12-17</td>
      <td>11731193</td>
      <td>306.0</td>
      <td>156.0</td>
      <td>506.0</td>
      <td>...</td>
      <td>0.11</td>
      <td>0.08</td>
      <td>0.14</td>
      <td>12.0</td>
      <td>9.4</td>
      <td>16.0</td>
      <td>NaN</td>
      <td>20.0</td>
      <td>15.0</td>
      <td>24.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1991-01-16</td>
      <td>12612043</td>
      <td>343.0</td>
      <td>178.0</td>
      <td>562.0</td>
      <td>...</td>
      <td>0.13</td>
      <td>0.11</td>
      <td>0.16</td>
      <td>17.0</td>
      <td>14.0</td>
      <td>20.0</td>
      <td>NaN</td>
      <td>96.0</td>
      <td>80.0</td>
      <td>110.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4</td>
      <td>EMR</td>
      <td>1992-03-20</td>
      <td>13811876</td>
      <td>371.0</td>
      <td>189.0</td>
      <td>614.0</td>
      <td>...</td>
      <td>0.16</td>
      <td>0.14</td>
      <td>0.18</td>
      <td>22.0</td>
      <td>19.0</td>
      <td>24.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>3 rows × 47 columns</p>
</div>




```python

```
