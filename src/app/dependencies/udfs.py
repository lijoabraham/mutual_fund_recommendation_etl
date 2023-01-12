from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col

def calculate_growth(df):
        growth_3m = 1 if df[0] > df[7] else 0
        growth_6m = 1 if df[1] > df[8] else 0
        growth_1y = 1 if df[2] > df[9] else 0
        growth_3y = 1 if df[3] > df[10] else 0
        growth_5y = 1 if df[4] > df[11] else 0
        growth_7y = 1 if df[5] > df[12] else 0
        growth_10y = 1 if df[6] > df[13] else 0

        g_vals = [6, 5, 4, 3, 2, 1]
        trend = 0
        for i in g_vals:
            diff = df[i - 1] - df[i]
            if diff > 0:
                trend = trend + 1
            elif diff < 0:
                trend = trend - 1
            # print(diff, df[i], df[i - 1], trend)

        growth = 7 - (growth_3m + growth_6m + growth_1y + growth_3y + growth_5y + growth_7y + growth_10y)
        trend_growth = 7 - trend

        return growth + trend_growth

def calculate_per_change(df):
        fund_low, cat_low =0, int(len(df)/2)
        g_vals = range(cat_low)
        tot_per_change = 0
        for i in g_vals:
            per_change = 0 if df[cat_low] == 0 else ((df[fund_low] - df[cat_low])/df[cat_low])*100 
            tot_per_change += per_change
            # print(per_change, tot_per_change, df[fund_low], df[cat_low])
            fund_low+=1
            cat_low+=1
        
        return int(tot_per_change)

def min_max_analysis(df, input_cols, min_val, max_val):
    input_column = "_".join(input_cols)
    assembler = VectorAssembler(inputCols=input_cols, 
    outputCol=f"vect_{input_column}")
    output = assembler.transform(df)
    
    scaler = MinMaxScaler(inputCol=f"vect_{input_column}", outputCol=f"scaled_{input_column}",min=min_val,max=max_val)
    # rescale each feature to range [min, max].
    scaledData = scaler.fit(output).transform(output)
    # scaledData.select([f"vect_{input_column}",f"scaled_{input_column}"]).show(2)
     
    scaledDatadf = scaledData.withColumn("scaled", vector_to_array(f"scaled_{input_column}"))
    # scaledDatadf.show(1)
    for i in range(len(input_cols)):
        scaledDatadf = scaledDatadf.withColumn(f'f_scaled_{input_cols[i]}',col("scaled")[i])
    # scaledDatadf.show(1)
    return scaledDatadf