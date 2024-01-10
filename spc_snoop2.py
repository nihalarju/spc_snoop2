import pandas as pd
import numpy as np
from scipy import interpolate
import matplotlib.pyplot as plt
import PyUber


def main():
    spices = SQL_DataFrame(sql)
    spices.to_csv('spices.csv', index=False)
    
def convert_to_date(df, column1='MEAS_SET_DATA_COLLECT_DATE', column2='LOT_DATA_COLLECT_DATE', \
                    column3='CURRENT_MOVEIN_DATE', column4='END_DATE'):
    if column1 in df.columns:
        df[column1] = pd.to_datetime(df[column1])
    if column2 in df.columns:
        df[column2] = pd.to_datetime(df[column2])
    if column3 in df.columns:
        df[column3] = pd.to_datetime(df[column3])
    if column4 in df.columns:
        df[column4] = pd.to_datetime(df[column4])
    return df

def SQL_DataFrame(sql, source='D1D_PROD_XEUS'):
    conn = PyUber.connect(source)
    df = pd.read_sql(sql, conn)
    return df


sql="""
SELECT  DISTINCT 
          a1.entity AS entity
         ,a5.value AS chart_value
         ,To_Char(a0.data_collection_time,'yyyy-mm-dd hh24:mi:ss') AS lot_data_collect_date
         ,a3.measurement_set_name AS measurement_set_name
         ,To_Char(a3.data_collection_time,'yyyy-mm-dd hh24:mi:ss') AS meas_set_data_collect_date
         ,a2.monitor_type AS monitor_type
         ,a3.parameter_class AS parameter_class
         ,a2.monitor_set_name AS monitor_set_name
         ,a0.lotoperkey AS lotoperkey
         ,a5.incontrol_flag AS incontrol_flag
         ,a5.standard_flag AS chart_pt_standard_flag
         ,a10.centerline AS centerline
         ,a10.lo_control_lmt AS lo_control_lmt
         ,a10.up_control_lmt AS up_control_lmt
         ,a5.chart_type AS chart_type
         ,a5.spc_chart_subset AS spc_chart_subset
         ,a2.test_name AS test_name
         ,a3.parameter_header AS parameter_header
         ,a2.module AS module
FROM 
P_SPC_MEASUREMENT_SET a3
INNER JOIN P_SPC_SESSION a2 ON a2.spcs_id = a3.spcs_id
LEFT JOIN P_SPC_LOT a0 ON a0.spcs_id = a2.spcs_id
INNER JOIN P_SPC_ENTITY a1 ON a2.spcs_id = a1.spcs_id AND a1.entity_sequence=1
LEFT JOIN P_SPC_CHART_POINT a5 ON a5.spcs_id = a3.spcs_id AND a5.measurement_set_name = a3.measurement_set_name
LEFT JOIN P_SPC_CHART_LIMIT a10 ON a10.chart_id = a5.chart_id AND a10.limit_id = a5.limit_id
WHERE
              a1.entity = 'PAT414_PM3' 
 AND      a5.value Is Not Null  
 AND      a3.data_collection_time >= TRUNC(SYSDATE) - 365 
 AND      a2.monitor_type = 'TOOL MONITOR' 
 AND      a3.parameter_class = 'REMOVAL_RATE' 
"""

#if __name__ == "__main__": main()


spices = SQL_DataFrame(sql)
er_spices = spices[spices['PARAMETER_CLASS']=='REMOVAL_RATE']
ers = er_spices
oxer = ers[((ers['TEST_NAME']=='4LATOXERPS')&(ers['CHART_TYPE']=='X-BAR'))]
a=oxer[['LOT_DATA_COLLECT_DATE', 'CHART_VALUE']]
a=a.rename(columns={'LOT_DATA_COLLECT_DATE': 't', 'CHART_VALUE': 'y'})
a=a.reset_index()[['t','y']]
a.index = pd.to_datetime(a.t)
a.drop(['t'], axis=1, inplace=True)
a