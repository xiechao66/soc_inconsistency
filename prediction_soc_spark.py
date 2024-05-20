# utf-8
"""
version: 3.0
date: 2024-01-09
update:
增加风险等级判断：
1、最近7天ΔSOC>20%
2、拐点里程<3000km&最近7天ΔSOC>X(X=15%)
3、存在特征点: 里程增长 < 50km && ΔSOC劣化 > 3%
4、存在特征点: 里程增长 < 500km && ΔSOC劣化 >10%
5、ΔSOC/100km 变化速率 > 0.3% && 拐点里程<3000km

预警详情：
1、高风险：详情描述：单体ASOC趋劣，最近7天max (SOC) =XX； 建议：请立即维修
2、中风险：ΔSOC>10%； 详情描述：单体ASOC离群，最近7天max (SOC) =XX； 建议：请联系供应商确认
3、低风险：ΔSOC>5%； 详情描述：单体ASOC离群，最近7天max (SOC) =XX； 建议：请联系供应商确认

"""


import sys
import time
import math
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import PandasUDFType, pandas_udf
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType, IntegerType, FloatType
from sklearn import linear_model
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import PolynomialFeatures
from datetime import datetime


# 拐点之后，根据里程滑窗，计算ΔSOC
def window_mileage(df_soc_data, online_result, bad_cell_i, mileage_threshold, soc_threshold, judge_label):

    # 获取从拐点开始/从最后5000公里开始  到最后一个点的斜率
    for index,row in online_result.iterrows():
        vin_i = row["vin"]
        df_vin_i_bad_cell_i = df_soc_data
        df_vin_i_bad_cell_i = df_vin_i_bad_cell_i.loc[:, ["vin" ,"endmileage", bad_cell_i]]
        df_vin_i_bad_cell_i = df_vin_i_bad_cell_i.sort_values(by=["endmileage"]).reset_index(drop=True)

        if df_vin_i_bad_cell_i.loc[0, bad_cell_i] >= 3.0:
            df_vin_i_bad_cell_i= pd.concat([pd.DataFrame({'vin': [vin_i], 'endmileage':[0.0], bad_cell_i: [0.0]}), df_vin_i_bad_cell_i])

        df_vin_i_bad_cell_i = df_vin_i_bad_cell_i.sort_values(by=["endmileage"]).reset_index(drop=True)
        df_vin_i_bad_cell_i["mileage_diff"] = df_vin_i_bad_cell_i["endmileage"].apply(lambda x: x-df_vin_i_bad_cell_i["endmileage"].tolist()[0])
        window_df = pd.DataFrame()
        for start in range(len(df_vin_i_bad_cell_i["mileage_diff"])):
            for end in range(start+1,len(df_vin_i_bad_cell_i["mileage_diff"]),1):
                if df_vin_i_bad_cell_i.iloc[end]['mileage_diff'] - df_vin_i_bad_cell_i.iloc[start]['mileage_diff'] < mileage_threshold:
                    window_data = df_vin_i_bad_cell_i.iloc[start:end+1].reset_index(drop=True)
                    soc_diff = np.max(window_data[bad_cell_i])-window_data.iloc[0][bad_cell_i]
                    window_df = window_df.append({'soc_diff': soc_diff, 'day_num': len(window_data[bad_cell_i])}, ignore_index=True)
                else:
                    window_df = window_df.append({'soc_diff': 0.0, 'day_num': 0}, ignore_index=True)
                    break

        if np.max(window_df['soc_diff']) > soc_threshold:
            online_result.loc[index, judge_label] = 1
            # online_result.loc[index, f'{judge_label}_day'] = np.max(window_df['day_num'])
        else:
            online_result.loc[index, judge_label] = 0
            # online_result.loc[index, f'{judge_label}_day'] = 0

    return online_result

# judge1，2,6
def judge_126 (online_result):
    for index, row in online_result.iterrows():
        bad_mile = row['bad_mile']
        bad_mile = float(str(bad_mile).split('~')[0])

        if row['maxSOC_7'] > 20:
            online_result.loc[index,'judge_1'] = 1
        else:
            online_result.loc[index,'judge_1'] = 0

        if (row['maxSOC_7']>15) & (bad_mile<3000.0):
            online_result.loc[index,'judge_2'] = 1
        else:
            online_result.loc[index,'judge_2'] = 0

    return online_result


def judge_5(df_soc_data, online_result):
    for index, row in online_result.iterrows():
        bad_mile = row['bad_mile']
        bad_mile = float(str(bad_mile).split('~')[0])
        if (bad_mile < 3000.0) & (bad_mile is not None):
            vin_i = row["vin"]
            bad_cell_i = row["bad_cell"].replace("U","avg")
            df_vin_i_bad_cell_i = df_soc_data[df_soc_data["vin"] == vin_i]
            df_vin_i_bad_cell_i = df_vin_i_bad_cell_i.loc[:, ["vin","date" ,"endmileage", bad_cell_i]]

            # 如果找到了拐点,则获取拐点之后的数据
            if isinstance(row["bad_day"], str) & (row["bad_day"] != 'None'):
                if "~" in row["bad_day"]:
                    bad_day = row["bad_day"].split("~")[1]
                else:
                    bad_day = row["bad_day"]
                df_vin_i_bad_cell_i =df_vin_i_bad_cell_i[df_vin_i_bad_cell_i["date"]>=bad_day]

            df_vin_i_bad_cell_i = df_vin_i_bad_cell_i.sort_values(by=["endmileage"]).reset_index(drop=True)
            soc_diff = np.max(df_vin_i_bad_cell_i[bad_cell_i])-df_vin_i_bad_cell_i.iloc[0][bad_cell_i]
            mileage_diff = df_vin_i_bad_cell_i.iloc[df_vin_i_bad_cell_i[bad_cell_i].idxmax()]["endmileage"] - df_vin_i_bad_cell_i.iloc[0]["endmileage"]
            if soc_diff/mileage_diff * 100 > 0.3:
                online_result.loc[index,'judge_5'] = 1
            else:
                online_result.loc[index,'judge_5'] = 0
        else:
            online_result.loc[index,'judge_5'] = 0

    return online_result


# 在返回第一个拐点时判断时间中断处的情况
def def_time_break_result(df_row):
    '''
    此函用用于判断是否满足时间断开时出现了劣化情况，这种无法给出准确拐点
    :param df_row: 对每一行
    :return: 是否是在数据中断时出现拐点的标签,出现返回1,未出现返回0
    '''
    # 如果与前一天的日期差没有超过15天,则不按照数据中断处理
    if not df_row["day_last_day_diff"] > 15:
        # print("无大于15的")
        return 0
    else:
        # 如果数据中断超过15天
        x_before = [float(item) for item in df_row["x_before"]]
        y_before = [float(item) for item in df_row["y_before"]]
        # 如果中断点前只有一个数据点，没有办法求斜率，则直接根据断点前后Δsoc差判断拐点是否在此处,中断点前小于3,中断点后大于5
        if len(y_before) == 1 and df_row["before_mean"] < 3 and df_row["after_mean"] > 5:
            # print("返回1")
            return 1
        # 如果中断点前有超过一个数据点,则直接拟合求中断点之前的斜率,如果斜率较小(小于0.05),且中断前的均值小于,且与断点后的ΔSOC差值较大(大于2),则认为此处存在拐点
        elif len(y_before) > 1:
            para = np.polyfit(x_before, y_before, 1)
            if para[0] < 0.05 and df_row["before_mean"] <= 5 and (df_row["after_mean"]-df_row["before_mean"]) > 2:
                # print("返回1")
                return 1
        else:
            # print("返回0")
            return 0

def get_first_inflection_point(df_bad_cell_i, bad_cell_i):

    # 对故障单体进行循环，找到突变点；
    bad_cell_i_value = df_bad_cell_i[bad_cell_i].astype('double').tolist()
    if all(num >= 5 for num in bad_cell_i_value):
        # 所有数据点均大于5%，不予以判断(返回开始已劣化,无拐点)
        bad_result = pd.DataFrame({'vin': [df_bad_cell_i['vin'].iloc[0]],
                                   'bad_cell': [bad_cell_i],
                                   'type': ['有数据时已劣化'],
                                   'bad_day': [df_bad_cell_i['date'].iloc[0].strftime('%Y-%m-%d')],
                                   'bad_mile': [df_bad_cell_i['endmileage'].iloc[0]]})
        bad_result['bad_day'] = pd.to_datetime(bad_result['bad_day'])
        bad_result['bad_day'] = bad_result['bad_day'].dt.strftime('%Y-%m-%d')
    else:
        start_date = df_bad_cell_i["date"].tolist()[0]
        if not isinstance(start_date, str):
            df_bad_cell_i["day_diff"] = df_bad_cell_i["date"].apply(lambda x: (x - start_date).days)
        # 如果不是时间格式，而是字符串，则需要转换为时间格式
        else:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            df_bad_cell_i["day_diff"] = df_bad_cell_i["date"].apply(lambda x: (datetime.strptime(x, '%Y-%m-%d') - start_date).days)

        # 数据中断处理算法简介
        # 中断有两个麻烦的点: 1.拐点正好在数据中断的地方   2.数据中断/大面积缺失导致拟合斜率偏差很大   因此需要对中断的情况做特殊处理
        # 基于day_diff找数据中断的点，如果day_diff上下两个时间差大于15天，则认为有数据中断，此时判断数据中断前后是否有数据点大幅度的变化,如果有,说明拐点在此处,直接给出
        # 判断左右两侧是否有大幅度变化的方式具体在time_break_result函数中
        # 滑窗求均值
        df_bad_cell_i["last_day_diff"] = df_bad_cell_i["day_diff"].shift(1)
        df_bad_cell_i.sort_values(by=["vin", "date"], ascending=True, inplace=True)

        df_bad_cell_i[f"last_{bad_cell_i}"] = df_bad_cell_i[bad_cell_i].shift(1)
        df_bad_cell_i["before_mean"] = df_bad_cell_i[f"last_{bad_cell_i}"].rolling(window=3, min_periods=1).mean()

        df_bad_cell_i.sort_values(by=["vin", "date"], ascending=False, inplace=True)
        df_bad_cell_i["after_mean"] = df_bad_cell_i[bad_cell_i].rolling(window=3, min_periods=1).mean()

        # 获取每一个点前后各6个点的数据和对应的day_diff,用于后续求断点前的斜率
        df_bad_cell_i.sort_values(by=["vin", "date"], ascending=True, inplace=True)
        df_bad_cell_i["day_last_day_diff"] = df_bad_cell_i["day_diff"] - df_bad_cell_i["last_day_diff"]
        df_bad_cell_i.reset_index(drop=False, inplace=True)

        y_all = df_bad_cell_i[bad_cell_i].tolist()
        df_bad_cell_i["y_all"] = df_bad_cell_i.apply(lambda x: y_all, axis=1)
        df_bad_cell_i["y_before"] = df_bad_cell_i.apply(lambda x: x["y_all"][max(int(x["index"] - 6), 0):max(int(x["index"]), 0)], axis=1)

        x_all = df_bad_cell_i["day_diff"].tolist()
        df_bad_cell_i["x_all"] = df_bad_cell_i.apply(lambda x: x_all, axis=1)
        df_bad_cell_i["x_before"] = df_bad_cell_i.apply(lambda x: x["x_all"][max(int(x["index"] - 6), 0):max(int(x["index"]), 0)], axis=1)

        # 开始执行时间中断判断,def_time_break_result函数,如果出现拐点则返回1,未出现则返回0(注意此拐点标签是打在断裂后的那个点上)
        df_bad_cell_i.sort_values(by=["vin", "date"], ascending=True, inplace=True)
        df_bad_cell_i["time_break_result"] = df_bad_cell_i.apply(lambda x: def_time_break_result(x), axis=1)
        # 如果已经根据中断时间判断出来,则给出故障日期范围和故障里程范围
        if (df_bad_cell_i["time_break_result"] == 1).any():
            # 获取上一帧的pt及里程,即断点的开始时间和开始里程; 本帧为断点的结束时间及结束里程
            df_bad_cell_i["last_date"] = df_bad_cell_i["date"].shift(1)
            df_bad_cell_i["last_mile"] = df_bad_cell_i["endmileage"].shift(1)
            df_bad_cell_i_break = df_bad_cell_i[df_bad_cell_i["time_break_result"] == 1]
            # 如果一台车出现了两个断点拐点,则直接选择第一个
            df_bad_cell_i_break.reset_index(drop=True,inplace=True)
            df_bad_cell_i_break = df_bad_cell_i_break.iloc[0,:]
            df_bad_cell_i_break["bad_day"] = df_bad_cell_i_break['last_date']
            df_bad_cell_i_break["bad_mile"] = df_bad_cell_i_break['last_mile']
            bad_result = pd.DataFrame({'vin': [df_bad_cell_i['vin'].iloc[0]],
                                       'bad_cell': [bad_cell_i],
                                       'type': ['时间中断处劣化'],
                                       'bad_day': [df_bad_cell_i_break["bad_day"]],
                                       'bad_mile': [df_bad_cell_i_break["bad_mile"]]})
            bad_result['bad_day'] = pd.to_datetime(bad_result['bad_day'])
            bad_result['bad_day'] = bad_result['bad_day'].dt.strftime('%Y-%m-%d')
            break_point=True

        else:
            break_point = False

            k_1 = pd.DataFrame(
                columns=["vin", "bad_cell", "k", "time_1", "time_2", "min", "max", "diff", "last_k", "quotient",
                         "k_diff",
                         "first_window_0-30_result", "first_window_15-45_result", "type", "bad_day", "bad_mile",
                         "keep_flag"])
            k_2 = pd.DataFrame(
                columns=["vin", "bad_cell", "k", "time_1", "time_2", "min", "max", "diff", "last_k", "quotient",
                         "k_diff",
                         "first_window_0-30_result", "first_window_15-45_result", "type", "bad_day", "bad_mile",
                         "keep_flag"])

            # 中断点附近不存在拐点,直接将中断点与原数据点拼起来,跳过数据中断的这部分,否则中断点处计算出来的斜率会因为数据点少带来异常波动
            df_bad_cell_i["day_last_day_diff_process"] = df_bad_cell_i["day_last_day_diff"].apply(lambda x: x - 1 if x > 15 else 0)
            df_bad_cell_i["day_last_day_diff_process"] = df_bad_cell_i["day_last_day_diff_process"].cumsum()
            df_bad_cell_i["day_diff_process"] = df_bad_cell_i["day_diff"] - df_bad_cell_i["day_last_day_diff_process"]
            # 判断第一个窗口是否已经存在拐点
            # 然后直接从第一个窗口开始往后一个一个窗口求斜率找到斜率开始大幅度变化的点;之前会先判断第一个窗口是否已经满足
            # 标记时间窗口（步长15，窗口长度30）
            df_bad_cell_i["rolling_1_flag"] = df_bad_cell_i["day_diff_process"].apply(lambda x: math.floor(x / 30))
            df_bad_cell_i["rolling_2_flag"] = df_bad_cell_i["day_diff_process"].apply(lambda x: math.floor((x - 15) / 30))

            # 对两个窗口求斜率
            flag_all = df_bad_cell_i["rolling_1_flag"].unique().tolist()
            flag_2_all = df_bad_cell_i["rolling_2_flag"].unique().tolist()[1:]
            if len(flag_2_all) == 0:
                bad_result = pd.DataFrame({'vin': [df_bad_cell_i['vin'].iloc[0]],
                                           'bad_cell': [bad_cell_i],
                                           'type': ['只有一个窗口'],
                                           'bad_day': [df_bad_cell_i['date'].iloc[0].strftime('%Y-%m-%d')],
                                           'bad_mile': [df_bad_cell_i['endmileage'].iloc[0]]})
                bad_result['bad_day'] = pd.to_datetime(bad_result['bad_day'])
                bad_result['bad_day'] = bad_result['bad_day'].dt.strftime('%Y-%m-%d')
            else:
                for flag_i, flag_2_i in zip(flag_all, flag_2_all):
                    df_bad_cell_flag_i = df_bad_cell_i[df_bad_cell_i["rolling_1_flag"] == flag_i]
                    y = df_bad_cell_flag_i.filter(regex='^avg', axis=1).iloc[:, 0].tolist()
                    y = [float(item) for item in y]
                    x = df_bad_cell_flag_i.loc[:, "day_diff_process"].tolist()
                    x = [float(item) for item in x]
                    min_day = df_bad_cell_flag_i[df_bad_cell_flag_i["rolling_1_flag"] == flag_i]["day_diff"].min()
                    max_day = df_bad_cell_flag_i[df_bad_cell_flag_i["rolling_1_flag"] == flag_i]["day_diff"].max()
                    min_day_day = df_bad_cell_flag_i[df_bad_cell_flag_i["rolling_1_flag"] == flag_i]["date"].min()
                    max_day_day = df_bad_cell_flag_i[df_bad_cell_flag_i["rolling_1_flag"] == flag_i]["date"].max()
                    # 如果数据点数大于1(按理说已经处理过数据中断的情况,不应该出现数据点数为1的情况)
                    if len(y) > 1:
                        para = np.polyfit(x, y, 1)

                        # 返回的列名 vin,故障单体,该窗口的斜率k,窗口开始标签,窗口结束标签,时间开始标签,时间结束标签,窗口ΔSOC最小值,窗口ΔSOC最大值,ΔSOC差值
                        this_k1 = pd.DataFrame([[df_bad_cell_i['vin'].iloc[0], bad_cell_i, para[0], min_day, max_day,
                                                 min_day_day, max_day_day, min(y), max(y), max(y) - min(y)]],
                                               columns=["vin", "bad_cell", "k", "time_1",
                                                        "time_2", "date_1", "date_2", "min", "max", "diff"])
                    else:
                        this_k1 = pd.DataFrame([[df_bad_cell_i['vin'].iloc[0], bad_cell_i, np.nan, min_day, max_day,
                                                 min_day_day, max_day_day, min(y), max(y), max(y) - min(y)]],
                                               columns=["vin", "bad_cell", "k", "time_1",
                                                        "time_2", "date_1", "date_2", "min", "max", "diff"])
                    print(this_k1)

                    # 对第二个窗口求斜率
                    df_bad_cell_flag_2_i = df_bad_cell_i[df_bad_cell_i["rolling_2_flag"] == flag_2_i]
                    y = df_bad_cell_flag_2_i.filter(regex='^avg', axis=1).iloc[:, 0].tolist()
                    y = [float(item) for item in y]

                    x = df_bad_cell_flag_2_i.loc[:, "day_diff_process"].tolist()
                    x = [float(item) for item in x]

                    if len(y)>1:
                        para = np.polyfit(x, y, 1)
                        min_day = df_bad_cell_flag_2_i[df_bad_cell_flag_2_i["rolling_2_flag"]==flag_2_i]["day_diff"].min()
                        max_day = df_bad_cell_flag_2_i[df_bad_cell_flag_2_i["rolling_2_flag"]==flag_2_i]["day_diff"].max()
                        min_day_day = df_bad_cell_flag_2_i[df_bad_cell_flag_2_i["rolling_2_flag"]==flag_2_i]["date"].min()
                        max_day_day = df_bad_cell_flag_2_i[df_bad_cell_flag_2_i["rolling_2_flag"] == flag_2_i]["date"].max()
                        this_k2 = pd.DataFrame([[df_bad_cell_i['vin'].iloc[0],bad_cell_i,para[0],min_day,max_day,min_day_day,max_day_day,min(y),max(y),max(y)-min(y)]],
                                               columns=["vin","bad_cell","k","time_1","time_2","date_1","date_2","min","max","diff"])
                    else:
                        this_k2 = pd.DataFrame([[df_bad_cell_i['vin'].iloc[0],bad_cell_i, np.nan,min_day,max_day, min_day_day,max_day_day,min(y), max(y), max(y) - min(y)]],
                                               columns=["vin", "bad_cell", "k","time_1", "time_2","date_1","date_2", "min", "max","diff"])
                    # 判断是否第一个窗口就已经劣化（最小偏差<3, 最大偏差-最小偏差>1.5）
                    if flag_i == 0 and flag_2_i == 0:
                        this_k1["first_window_0-30_result"] = this_k1.apply(lambda x: 1 if float(x["min"]) < 3 and float(x["diff"]) > 1.5 else 0,axis=1)
                        this_k2["first_window_15-45_result"] = this_k2.apply(lambda x: 1 if float(x["min"]) < 3 and float(x["diff"]) > 1.5 else 0,axis=1)
                        this_k1["type"] = this_k1.apply(lambda x: "第一个窗口开始劣化" if x["first_window_0-30_result"] == 1 else "null", axis=1)
                        this_k2["type"] = this_k2.apply(lambda x: "第一个窗口开始劣化" if x["first_window_15-45_result"] == 1 else "null", axis=1)
                    else:
                        this_k1["first_window_0-30_result"] = 0
                        this_k2["first_window_15-45_result"] = 0
                        this_k1["type"] = this_k1.apply(lambda x: "第一个窗口开始劣化" if x["first_window_0-30_result"] == 1 else "null", axis=1)
                        this_k2["type"] = this_k2.apply(lambda x: "第一个窗口开始劣化" if x["first_window_15-45_result"] == 1 else "null", axis=1)

                    k_1 = k_1.append(this_k1)
                    k_2 = k_2.append(this_k2)

                # 在该故障单体所有窗口循环完成后
                k_1["last_k"] = k_1["k"].shift(1)
                k_1["quotient"] = abs(k_1["k"]/k_1["last_k"])
                k_1["k_diff"] = k_1["k"] - k_1["last_k"]

                k_2["last_k"] = k_2["k"].shift(1)
                k_2["quotient"] = abs(k_2["k"]/k_2["last_k"])
                k_2["k_diff"] = k_2["k"] - k_2["last_k"]

                k_all_this = k_1.append(k_2)

                # 寻找type为空且斜率大于0.05的窗口,按照变化倍数倒叙排序,选择第一个,即变化系数最大的窗口,将其加入到结果集中
                new_column = k_all_this.apply(lambda x: 1 if (x["type"] != "null" or x["k"] > 0.05) else 0, axis=1)
                print(new_column)
                print(k_all_this)
                k_all_this["keep_flag"] = new_column
                k_all_this = k_all_this[k_all_this["keep_flag"] == 1]

                k_all_this = k_all_this.sort_values(by=["vin", "type", "quotient"], ascending=False).drop_duplicates(["vin"], keep="first")
                if k_all_this.shape[0] == 0:
                    bad_result = pd.DataFrame({'vin': [df_bad_cell_i['vin'].iloc[0]],
                                               'bad_cell': [bad_cell_i],
                                               'type': ['获取到的数据内未识别到拐点'],
                                               'bad_day': [df_bad_cell_i['date'].iloc[0].strftime('%Y-%m-%d')],
                                               'bad_mile': [df_bad_cell_i['endmileage'].iloc[0]]})
                    # print("未发现拐点")
                else:
                    # 确定窗口后,在窗口内用全部点求斜率,如果斜率大于0.03,则删除掉窗口内的最后一个点,再求斜率,如此循环直到斜率小于0.03
                    fine_position_df = df_bad_cell_i[(df_bad_cell_i["date"]>=k_all_this["date_1"].tolist()[0])&(df_bad_cell_i["date"]<=k_all_this["date_2"].tolist()[0])]
                    fine_position_df.sort_values(by=["date"]).reset_index(drop=True,inplace=True)

                    find_flag = 0
                    times = fine_position_df.shape[0]
                    for i in range(times):
                        # 循环求斜率k,一个一个往后减
                        x = fine_position_df["day_diff_process"].tolist()[:times-i]
                        y = fine_position_df[bad_cell_i].tolist()[:times-i]
                        x = [float(item) for item in x]
                        y = [float(item) for item in y]

                        if len(y)<=1:
                            break
                        para = np.polyfit(x,y,1)
                        if abs(para[0])>0.03:
                            continue

                        else:
                            fine_position_point_date = max(fine_position_df["date"].tolist()[:times-i])
                            fine_position_point_mile = max(fine_position_df["endmileage"].tolist()[:times-i])
                            k_all_this["bad_day"] = fine_position_point_date
                            k_all_this["bad_mile"] = fine_position_point_mile
                            find_flag = 1
                            break
                    if find_flag == 0:
                        fine_position_point_date = fine_position_df["date"].tolist()[0]
                        fine_position_point_mile = fine_position_df["endmileage"].tolist()[0]
                        k_all_this["bad_day"] = fine_position_point_date
                        k_all_this["bad_mile"] = fine_position_point_mile

                    bad_result = pd.DataFrame({'vin': [df_bad_cell_i['vin'].iloc[0]],
                                               'bad_cell': [bad_cell_i],
                                               'type': ['基于斜率识别拐点'],
                                               'bad_day': [k_all_this["bad_day"][0]],
                                               'bad_mile': [k_all_this["bad_mile"][0]]})
                    bad_result['bad_day'] = pd.to_datetime(bad_result['bad_day'])
                    bad_result['bad_day'] = bad_result['bad_day'].dt.strftime('%Y-%m-%d')

    bad_result["bad_cell"] = bad_result["bad_cell"].apply(lambda x: x.replace("avg","U"))

    return bad_result
def res_schema() -> StructType:
    return StructType([StructField('vin', StringType(), True),
                       StructField('car_type', StringType(), True),
                       StructField('declare_type', StringType(), True),
                       StructField('pkg_type', StringType(), True),
                       StructField('startDate', StringType(), True),
                       StructField('endDate', StringType(), True),
                       StructField('startSOC', DoubleType(), True),
                       StructField('endSOC', DoubleType(), True),
                       StructField('maxSOC', DoubleType(), True),
                       StructField('date30', StringType(), True),
                       StructField('cell', StringType(), True),
                       StructField('endmileage', DoubleType(), True),
                       StructField('level', IntegerType(), True),
                       StructField('judge_1', DoubleType(), True), #这里改为DoubleType()
                       StructField('judge_2', DoubleType(), True), #这里改为DoubleType()
                       StructField('judge_3', DoubleType(), True), #这里改为DoubleType()
                       StructField('judge_4', DoubleType(), True), #这里改为DoubleType()
                       StructField('judge_5', DoubleType(), True), #这里改为DoubleType()
                       StructField('warn_level', IntegerType(), True),
                       StructField('type', StringType(), True),
                       StructField('bad_day', StringType(), True),
                       StructField('bad_mile', DoubleType(), True)
                       ])


@pandas_udf(returnType=res_schema(), functionType=PandasUDFType.GROUPED_MAP)
def linear_regression(df: pd.DataFrame):
    df = df.apply(pd.to_numeric, errors='ignore')
    df["date"] = df["date"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    df = df.sort_values(by=['vin', 'date']).reset_index(drop=True)
    df['day'] = (df['timeStamp'] - df['timeStamp'][0]) // 86400
    df_7 = df[df['timeStamp'] >= (pt_timestamp - 86400 * 8)].reset_index(drop=True)  # 最近7天的数据
    df_42= df[df['timeStamp'] >= (pt_timestamp - 86400 * 43)].reset_index(drop=True)  # 最近42天数据

    cell_num = 107
    avg_column_all = [f"avg{i + 1}" for i in range(cell_num)]

    # 获取除单体列之外的其他列
    other_columns = [x for x in df.columns.tolist() if x not in avg_column_all]
    # 筛选存在大于3%的列(单体)，过滤数据以减小数据量
    mask_precent_3 = df_42.loc[:, avg_column_all].astype('double').gt(5).any()
    mask_precent_3 = mask_precent_3[mask_precent_3 == True].index.tolist()
    vin = df_42['vin'].iloc[0]
    car_type = df_42['car_type'].iloc[0]
    declare_type = df_42['declare_type'].iloc[0]
    pkg_type = df_42['pkg_type'].iloc[0]
    startDate = df_42['date'].iloc[0].strftime('%Y-%m-%d')
    endDate = df_42['date'].iloc[-1].strftime('%Y-%m-%d')
    endmileage = df_42['endmileage'].iloc[-1]
    print(vin)
    if len(mask_precent_3) >= 5:
        result = pd.DataFrame({'vin': [vin],
                               'car_type': [car_type],
                               'declare_type': [declare_type],
                               'pkg_type': [pkg_type],
                               'startDate': [startDate],
                               'endDate': [endDate],
                               'startSOC': [0.0],
                               'endSOC': [0.0],
                               'maxSOC': [0.0],
                               'date30': ['None'],
                               'cell': ['PACK'],
                               'endmileage': [endmileage],
                               'level': [-1],
                               'judge_1': [0],
                               'judge_2': [0],
                               'judge_3': [0],
                               'judge_4': [0],
                               'judge_5': [0],
                               'warn_level': [1],
                               'type': ['None'],
                               'bad_day': ['None'],
                               'bad_mile': [0.0]})


    elif mask_precent_3 != []:

        result = pd.DataFrame(data=None,
                              columns=['vin', 'car_type', 'declare_type', 'pkg_type',
                                       'startDate', 'endDate', 'startSOC', 'endSOC', 'maxSOC', 'date30',
                                       'cell', 'endmileage', 'level', 'judge_1', 'judge_2', 'judge_3', 'judge_4', 'judge_5', 'warn_level',
                                       'type', 'bad_day', 'bad_mile'])

        for bad_cell_i in mask_precent_3:
            y = df_7[bad_cell_i].astype('double').to_numpy()
            index = np.where(y >= 30)[0]
            diff = df_42[bad_cell_i] - df_42[bad_cell_i].shift(1)
            diff1 = df_42[bad_cell_i] - df_42[bad_cell_i].shift(2)
            need_columns = other_columns + [bad_cell_i]
            df_bad_cell_i = df.loc[:, need_columns].sort_values(by="date").reset_index(drop=True)
            inflection_point = get_first_inflection_point(df_bad_cell_i, bad_cell_i)
            inflection_point["maxSOC_7"] = df_7[bad_cell_i].max()
            inflection_point["pkg_type"] = [df['pkg_type'].iloc[0]]

            # 风险等级判断
            # SWD_list = SWD_list_broadcast.value47
            online_result_with_point = judge_126(inflection_point)
            online_result_with_point = window_mileage(df_bad_cell_i, online_result_with_point, bad_cell_i, 50, 3, 'judge_3')
            online_result_with_point = window_mileage(df_bad_cell_i, online_result_with_point, bad_cell_i, 500, 10, 'judge_4')
            online_result_with_point = judge_5(df_bad_cell_i, online_result_with_point)

            if ((online_result_with_point[['judge_1', 'judge_2', 'judge_3', 'judge_4', 'judge_5']] == 1).any(axis=1)).any():
                online_result_with_point['warn_level'] = 1
            else:
                online_result_with_point['warn_level'] = 0
            print([item for item in online_result_with_point])

            print([online_result_with_point[item][0] for item in online_result_with_point])

            startSOC = df[bad_cell_i].iloc[0]
            endSOC = df[bad_cell_i].iloc[-1]
            maxSOC = df_7[bad_cell_i].max()

            if (np.min(diff, ) <= -4.0) | (np.min(diff1, ) <= -4.0):
                print("1")
                date30 = 'None'
                level = 4

            elif index.size != 0:
                print("2")
                date30 = df_7['date'].iloc[index[0]].strftime('%Y-%m-%d')
                level = 3

            elif np.where(y >= 10)[0].size != 0:
                print("3")
                df_42['diff2'] = df_42[bad_cell_i].rolling(window=3, center=True, min_periods=1).mean()
                clf = Pipeline(
                    [('poly', PolynomialFeatures(degree=1)), ('linear', linear_model.LinearRegression())])

                x = df_42['day'].to_numpy()
                y = df_42['diff2'].to_numpy()
                clf.fit(x[:, np.newaxis], y)
                x1 = np.arange(x[-1], x[-1] + 90)
                y1 = clf.predict(x1[:, np.newaxis])

                index = np.where(y1 >= 30)[0]
                if index.size != 0:
                    predict_date = pd.Series((x1 - x[-1]) * 86400 * 1e9 + df['timeStamp'].iloc[-1] * 1e9)
                    predict_date = pd.to_datetime(predict_date, format='%Y-%m-%d')
                    date30 = predict_date.iloc[index[0]].strftime('%Y-%m-%d')
                    level = 2

                else:
                    date30 = 'None'
                    level = 1

            elif np.where(y >= 5)[0].size != 0:
                print("4")
                date30 = 'None'
                level = 0

            else:
                print("5")
                date30 = 'None'
                level = -1
                maxSOC = 0.0
            predict_data = pd.DataFrame({'vin': [vin],
                                         'car_type': [car_type],
                                         'declare_type': [declare_type],
                                         'pkg_type': [pkg_type],
                                         'startDate': [startDate],
                                         'endDate': [endDate],
                                         'startSOC': [startSOC],
                                         'endSOC': [endSOC],
                                         'maxSOC': [maxSOC],
                                         'date30': [date30],
                                         'cell': [bad_cell_i],
                                         'endmileage': [endmileage],
                                         'level': [level],
                                         'judge_1': [online_result_with_point['judge_1'][0]],
                                         'judge_2': [online_result_with_point['judge_2'][0]],
                                         'judge_3': [online_result_with_point['judge_3'][0]],
                                         'judge_4': [online_result_with_point['judge_4'][0]],
                                         'judge_5': [online_result_with_point['judge_5'][0]],
                                         'warn_level': [online_result_with_point['warn_level'][0]],
                                         'type': [online_result_with_point['type'][0]],
                                         'bad_day': [online_result_with_point['bad_day'][0]],
                                         'bad_mile': [online_result_with_point['bad_mile'][0]]})

            print(predict_data.dtypes)
            print([predict_data[col][0] for col in predict_data.columns])
            result = result.append(predict_data, ignore_index=True).reset_index(drop=True)
            result["cell"] = result["cell"].apply(lambda x: x.replace("avg","U"))
    else:
        print("6")
        result = pd.DataFrame({'vin': [vin],

                               'car_type': [car_type],
                               'declare_type': [declare_type],
                               'pkg_type': [pkg_type],
                               'startDate': [startDate],
                               'endDate': [endDate],
                                 'startSOC': [0.0],
                                 'endSOC': [0.0],
                                 'maxSOC': [0.0],
                                 'date30': ['None'],
                                 'cell': ['None'],
                               'endmileage': [endmileage],
                                 'level': [-1],
                                 'judge_1': [0],
                                 'judge_2': [0],
                                 'judge_3': [0],
                                 'judge_4': [0],
                                 'judge_5': [0],
                                 'warn_level': [0],
                                 'type': [0],
                                 'bad_day': [0],
                                 'bad_mile': [0]})

    return result


if __name__ == '__main__':
    # 设置SparkSession
    spark = SparkSession.builder.appName('predict_model').master("yarn").enableHiveSupport() \
        .config("dfs.namenode.acls.enabled", 'false') \
        .config("hive.exec.dynamic.partition;", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    spark.conf.set('spark.sql.execution.arrow.enabled', 'true')

    pt = sys.argv[1]  # 当天pt yyyyMMdd
    print(pt)
    pt1 = sys.argv[2]  # 昨天pt yyyyMMdd-1
    cell = sys.argv[3]

    pt_timestamp = time.mktime(time.strptime(pt, "%Y%m%d"))
    print(pt_timestamp)

    outpath = "obs://obs-ews-dev/SOC不一致预警/files/warn"

    origin = spark.sql("""
    SELECT * FROM viree_forward_search_dev.fs_dws_soc_carFilter_v3_wi
    WHERE pt = '%s'
    """ % pt).select("*",*[(F.col("delta_soc")[i]).alias(str(f"avg{i+1}")) for i in range(int(cell))]).drop("delta_soc")

    # 读取车辆静态信息表
    # 从车辆静态信息表读取车架号-车型-公告号-电池包型号信息
    # 读取电池包SN码
    vin_info = spark.sql(f"""
        SELECT
            vin
            , car_type
            , declare_type
            , pkg_type
            , extra_3 AS battery_pack_code
        FROM dim_batt.fs_dim_vehicle_info_df
        WHERE pt IN (SELECT max(pt) FROM dim_batt.fs_dim_vehicle_info_df)
        """)

    # 最后报警位置
    latest_data = spark.sql(f"""
            SELECT vin
                    , province
                    , city
            FROM dwd_batt.fs_dwd_gb_vehicle_latest_data_di
            WHERE pt = '{pt1}'
        """)
    origin = origin.withColumn('timeStamp', F.unix_timestamp(F.col('date'), 'yyyy-MM-dd').cast('long'))\
        .join(vin_info, on=["vin"], how="left_outer")
    # origin.show()

    # # PandasUDF中进行线性回归
    res = origin\
        .groupby("vin").apply(linear_regression)
    # res.show()

    res.persist()

    result = res.where(F.col("level") >= 0) \
        .withColumn("warn_level", F.when(F.col("warn_level") == 1, F.lit("高"))
                    .when(F.col("maxSOC") >= 10.0, F.lit("中"))
                    .when(F.col("maxSOC") >= 5.0, F.lit("低"))) \
        .withColumn("longitude", F.lit('None')) \
        .withColumn("latitude", F.lit('None')) \
        .withColumn("start_time", F.col("startDate")) \
        .withColumn("end_time", F.col("endDate")) \
        .withColumn("abnormal_units", F.col("cell")) \
        .withColumn('maxSOC', F.round(F.col('maxSOC'), 2)) \
        .withColumn("suggestion", F.when(F.col("warn_level") == "高", F.lit("请立即维修"))
                    .when(F.col("warn_level") == "中", F.lit("请联系供应商确认"))
                    .when(F.col("warn_level") == "低", F.lit("请联系供应商确认"))) \
        .withColumn("mileage", F.round(F.col("endmileage"), 1)) \
        .withColumn("fault_details", F.when(F.col("cell") == "PACK", F.lit("整包一致性差"))
                    .when(F.col("warn_level") == "高", F.concat(F.lit("单体ΔSOC趋劣, 最近7天Max(ΔSOC)="), F.col("maxSOC").cast("string"), F.lit('%')))
                    .when(F.col("warn_level") == "中", F.concat(F.lit("单体ΔSOC离群, 最近7天Max(ΔSOC)="), F.col("maxSOC").cast("string"), F.lit('%')))
                    .when(F.col("warn_level") == "低", F.concat(F.lit("单体ΔSOC离群, 最近7天Max(ΔSOC)="), F.col("maxSOC").cast("string"), F.lit('%')))) \
        .withColumn("warn_kind", F.lit("18"))\
        .join(latest_data, on=["vin"], how="left_outer")\
        .withColumn("warn_location", F.concat(F.col("province"), F.col("city")))
    res.unpersist()
    result.persist()

    # result.\
    #     repartition(1).write.mode("append").format("csv").option("header", True).save(outpath + "/warn_test")

    result.createTempView('predict_detail')
    spark.sql("""
        INSERT OVERWRITE TABLE dws_batt.fs_dws_soc_consistency_predict_v3_wi PARTITION (pt='%s')
        SELECT vin,
            abnormal_units,
            level,
            warn_level,
            startDate AS start_time,
            endDate AS end_time,
            suggestion,
            mileage,
            fault_details,
            warn_location,
            judge_1,
            judge_2,
            judge_3,
            judge_4,
            judge_5,
            type,
            bad_day,
            bad_mile AS bad_mileage,
            warn_kind,
            car_type,
            pkg_type,
            declare_type
        FROM predict_detail
        """ % pt)

    result.where(F.col("level") != 4).createTempView('predict_result')
    spark.sql("""
        INSERT OVERWRITE TABLE dws_batt.fs_dws_soc_consistency_predict_wi PARTITION (pt='%s')
        SELECT vin,
            warn_level,
            longitude,
            latitude,
            startDate AS start_time,
            endDate AS end_time,
            abnormal_units,
            suggestion,
            mileage,
            fault_details,
            warn_kind,
            car_type,
            pkg_type,
            declare_type
        FROM
            predict_result
    """ % pt)

    result.unpersist()



