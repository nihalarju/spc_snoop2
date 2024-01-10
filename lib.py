
def make_fig(st, fname):
    fig = plt.figure()
    ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    #ax = plt.subplot(111)
    x = np.linspace(-len(st)+1,0,len(st)).reshape(-1,1)*3
    ax.plot(x,st.values, 'o-')
    ax.legend([entity+': '+dstr])
    
    # PLOT spline
    try: 
        xn = np.linspace(x[0], x[-1], 100)
        non_fliers = st.values.astype(float)<4
        yp = st[non_fliers]
        xp = x[non_fliers]
        y_BSpline = interpolate.UnivariateSpline(xp,yp,s=20.)
        yn = y_BSpline(xn)
        ax.plot(xn, yn, '-')
    except:
        pass
    
    ax.set_xlabel('day')
    ax.set_ylabel('TA')
    ax.set_xticks([-60,-30,0])
    plt.ylim([-0.1, 4])
    
    fig.savefig('figs/'+fname+'.png')
    plt.close()


def get90d(ss, datestr): 
    end = pd.Timestamp(datestr) + timedelta(days=1)
    start = end - timedelta(days=90)
    try:
        ss = ss[(ss['LOT_DATA_COLLECT_DATE'] >= start) & (ss['LOT_DATA_COLLECT_DATE'] < end)]
    except:
        ss = convert_to_date(ss)
        ss = ss[(ss['LOT_DATA_COLLECT_DATE'] >= start) & (ss['LOT_DATA_COLLECT_DATE'] < end)]
    return ss


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


resample_20d(st, entity, tools, baselines, ):
	try:
		#tools.loc[entity].values[0]
		fname = tools.loc[entity]['REV_MODULE']+'.TA.'+entity+'.'+dstr 
		if len(fname)==2: fname = fname.values[0]
		baseline = baselines.loc[tools.loc[entity]['REV_MODULE']].values
	except:
		fname = 'NONE.TA.'+entity+'.'+dstr
		baseline = baselines['TA'].mean()
	sst = ss[ss['ENTITY']==entity]
	st = sst[sst['SPC_CHART_SUBSET'] == 'PARTICLE_SIZE=TOTAL_ADDERS']

	st=st[['LOT_DATA_COLLECT_DATE', 'CHART_VALUE']]
	st=st.rename(columns={'LOT_DATA_COLLECT_DATE': 't', 'CHART_VALUE': fname})
	st.index = pd.to_datetime(st.t)
	st.drop(['t'], axis=1, inplace = True)
	if len(st)<20: continue # need enough data to interpolate properly
	st=st.sort_index()
	st=st.resample('3D').mean()
	try:
		st=st.interpolate(method='spline', order=2)
		st[st<0] = 0
	except:
		continue
	#st=np.log(st+1)
	st = st[len(st)-20:]
	if len(st)<20: continue #sometimes data doesn't extend back 60 days
	#print(fname+' len: ', len(st))
	
	st = st/baseline
	return st

