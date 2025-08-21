# ---------------------------------------------------------------------------------------------------------------------------------------------#
# rounds to the nearest muliple of the number and multiple provided
def round_to_multiple(number, multiple):
    return multiple * round(number / multiple)


# gets all multiples of a number within a range of values while also under the maximum allowed value
def getMultiplesInRange(multiple, max, lowLimit, highLimit):
    allMultiples = list(range(multiple, (ms.floor(max)+1)*multiple, multiple))
    rangeMultiples = list(filter(lambda x: ms.ceil(lowLimit) <= x <= ms.floor(highLimit), allMultiples))
    
    return rangeMultiples


# ---------------------------------------------------------------------------------------------------------------------------------------------#
# used to shrink a curve -- a change matrix as well as a friction factor table is needed 
# note that the ratio for the shrink change matrix must be less than 1.0
#
# change matrix should mirror the following format:
#   changeMatrix = pd.DataFrame([
#       ['GC', 'MD' , 2, 0.5 , 8 ],
#       ['GC', 'HV' , 2, 0.5 , 8 ]
#   ],columns=(['TLFTYPE','TRIPPURP','BINSIZE','RATIO','ShrinkAfter'])) 
#
# dfNextRoundFF should have the following columns:
#   - BIN
#   - FF_CALIB_ROUND
#   - OAS
#   - TLFTYPE
#   - TRIPPURP
#   - FF 
def shrinkFFCurve(changeMatrix, dfNextRoundFF):
    
    # merge the change matrix to get the changes needed placed in columns in the dataframe
    dfChange = pd.DataFrame.merge(dfNextRoundFF, changeMatrix, on=(['TLFTYPE','TRIPPURP']))
    
    # shift FF and BIN up to new column, multiple the bin by the ratio, round the ratio bin to the nearest multiple
    df1 = dfChange
    df1['FF2'] = df1.groupby(['FF_CALIB_ROUND','TLFTYPE','TRIPPURP'])['FF'].shift(-1)
    df1['BINR'] = df1.apply(lambda x: x['BIN'] * x['RATIO'],axis=1)
    df1['BINR2'] = df1.groupby(['FF_CALIB_ROUND','TLFTYPE','TRIPPURP'])['BINR'].shift(-1)
    df1['NearestBIN'] = df1.apply(lambda x: round_to_multiple(x['BINR'],x['BINSIZE']),axis=1)

    # filter down to only new bins within original binsize or in the "beginning range"
    df2 = df1.query('BINR <= NearestBIN <= BINR2 | BIN <= ShrinkAfter')
    df2 = df2.dropna()
    df2['NewFF'] = df2.apply(lambda x: np.interp(x['NearestBIN'],[x['BINR'],x['BINR2']],[x['FF'],x['FF2']]), axis = 1)
    df2['NearestBIN'] = df2.apply(lambda x: x['BIN'] if x['BIN'] <= x['ShrinkAfter'] else x['NearestBIN'], axis=1)
    
    # drop duplicate bins and ensure FF is same for the "beginning range", also add new column for merging
    df3 = df2.drop_duplicates(['FF_CALIB_ROUND','TLFTYPE','TRIPPURP','NearestBIN'])
    df3['NewFF'] = df3.apply(lambda x: x['FF'] if x['BIN'] <= x['ShrinkAfter'] else x['NewFF'], axis=1)
    df3['TLFTYPE_TRIPPURP'] = df3['TLFTYPE'] + '_' + df3['TRIPPURP']

    # filter out all variables that were changed from original ff dataframe, also add new column for mergin
    dfM = dfNextRoundFF
    dfM['TLFTYPE_TRIPPURP'] = dfM['TLFTYPE'] + '_' + dfM['TRIPPURP']
    dfM2 = dfM[~dfM.TLFTYPE_TRIPPURP.isin(df3['TLFTYPE_TRIPPURP'])]

    # drop unneeded columns, rename columns, and merge on new "shrunken" ff dataframne to ff factor dataframe
    df4 = df3.drop(['BIN','FF', 'BINSIZE', 'RATIO', 'ShrinkAfter', 'FF2', 'BINR', 'BINR2'], axis = 1)
    df4 = df4.rename(columns={'NearestBIN':'BIN','NewFF':'FF'})   
    dfO = pd.concat([dfM2,df4])

    return dfO


# ---------------------------------------------------------------------------------------------------------------------------------------------#
# used to expand a curve -- a change matrix as well as a friction factor table is needed 
# Note the ratio for the expand change matrix must be greater than 1.0
#
#   changeMatrix = pd.DataFrame([
#       ['GC', 'MD' , 2, 1.5 , 8 ],
#       ['GC', 'HV' , 2, 1.5 , 8 ]
#   ],columns=(['TLFTYPE','TRIPPURP','BINSIZE','RATIO','ShrinkAfter'])) 
#
# dfNextRoundFF should have the following columns:
#   - BIN
#   - FF_CALIB_ROUND
#   - OAS
#   - TLFTYPE
#   - TRIPPURP
#   - FF 
def expandFFCurve(changeMatrix, dfNextRoundFF):
    
    # merge the change matrix to get the changes needed placed in columns in the dataframe
    dfChange = pd.DataFrame.merge(dfNextRoundFF, changeMatrix, on=(['TLFTYPE','TRIPPURP']))

    # shift FF and BIN up to new column, multiple the bin by the ratio, create a max column displaying the final new bin
    dfT1 = dfChange
    dfT1['FF2'] = dfT1.groupby(['FF_CALIB_ROUND','TLFTYPE','TRIPPURP'])['FF'].shift(-1)
    dfT1['BINR'] = dfT1.apply(lambda x: x['BIN'] * x['RATIO'],axis=1)
    dfT1['BINR2'] = dfT1.groupby(['FF_CALIB_ROUND','TLFTYPE','TRIPPURP'])['BINR'].shift(-1)
    dfT1['max'] = dfT1.groupby(['FF_CALIB_ROUND','TLFTYPE','TRIPPURP'])['BINR2'].transform('max')

    # calculate all binsize multiples within the expanded ranges and "explode" the dataframe
    dfT2 = dfT1.dropna()
    dfT2['BINList'] = dfT2.apply(lambda x: getMultiplesInRange(x['BINSIZE'],x['max'],x['BINR'],x['BINR2']),axis=1)
    dfT3 = dfT2.explode('BINList')

    # linerally interpolate new bins based on ff values, drop duplicates, and maintain values before the ExpandAfter value
    dfT3['NewFF'] = dfT3.apply(lambda x: np.interp(x['BINList'],[x['BINR'],x['BINR2']],[x['FF'],x['FF2']]), axis = 1)
    dfT3['BINList'] = dfT3.apply(lambda x: x['BIN'] if x['BIN'] <= x['ExpandAfter'] else x['BINList'], axis=1)
    dfT4 = dfT3.drop_duplicates(['FF_CALIB_ROUND','TLFTYPE','TRIPPURP','BINList'])
    dfT4['NewFF'] = dfT4.apply(lambda x: x['FF'] if x['BIN'] <= x['ExpandAfter'] else x['NewFF'], axis=1)

    # interpolate within the "forgotten range", which exists between the maintained beginning values and the new expanded values
    dfT4['leadBINList'] = dfT4.groupby(['FF_CALIB_ROUND','TLFTYPE','TRIPPURP'])['BINList'].shift(-1)
    dfT4['leadNewFF'] = dfT4.groupby(['FF_CALIB_ROUND','TLFTYPE','TRIPPURP'])['NewFF'].shift(-1)
    dfT4['missingList'] = dfT4.apply(lambda x: getMultiplesInRange(x['BINSIZE'],x['max'],x['BINList'],(x['leadBINList'] - x['BINSIZE'])) if x['BINList'] == x['ExpandAfter'] else [x['BINList']], axis=1)
    dfT5 = dfT4.explode('missingList').dropna()
    dfT5['NewFF2'] = dfT5.apply(lambda x: np.interp(x['missingList'],[x['BINList'],x['leadBINList']],[x['NewFF'],x['leadNewFF']]), axis = 1)
    dfT5['TLFTYPE_TRIPPURP'] = dfT5['TLFTYPE'] + '_' + dfT5['TRIPPURP']

    # select wanted columns and rename to original setup
    dfT6 = dfT5.filter(['missingList','FF_CALIB_ROUND','OAS','TLFTYPE','TRIPPURP','TLFTYPE_TRIPPURP','NewFF2'])
    dfT6 = dfT6.rename(columns={'missingList':'BIN','NewFF2':'FF'})

    # merge on new "expanded" values to original dataframe
    dfTM = dfNextRoundFF
    dfTM['TLFTYPE_TRIPPURP'] = dfTM['TLFTYPE'] + '_' + dfTM['TRIPPURP']
    dfTM2 = dfTM[~dfTM.TLFTYPE_TRIPPURP.isin(dfT6['TLFTYPE_TRIPPURP'])]
    dfTO = pd.concat([dfTM2,dfT6])

    return dfTO
