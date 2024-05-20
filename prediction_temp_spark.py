import builtins
import datetime
import time
import sys

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import PandasUDFType, pandas_udf, unix_timestamp, col, lit, when, concat, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from sklearn import linear_model
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import PolynomialFeatures


def res_schema() -> StructType:
    return StructType([StructField('vin', StringType(), False), StructField('startDate', StringType(), False),
                       StructField('endDate', StringType(), False), StructField('startTemp', DoubleType(), False),
                       StructField('endTemp', DoubleType(), False), StructField('date30', StringType(), False),
                       StructField('cell', IntegerType(), False)])


@pandas_udf(returnType=res_schema(), functionType=PandasUDFType.GROUPED_MAP)
def linear_regression(df: pd.DataFrame):
    # 数据预处理：
    # 找出平均单体偏差最大的单体编号
    # 仅保留上述单体编号和平均单体偏差最大值
    # 对上述偏差值做滑动平均滤波，滑动窗口大小为3
    df = df.dropna(axis=1).sort_values(by=['vin', 'date']).reset_index(drop=True)
    cell_num = len(df.columns) - 7
    df['maxID'] = df[['avg' + str(i + 1) for i in range(cell_num)]].abs().idxmax(axis=1).str.lstrip('avg').astype('int')
    df['day'] = (df['timeStamp'] - df['timeStamp'][0]) // 86400
    df = df[['vin', 'date', 'maxDiff', 'maxID', 'timeStamp', 'day']]
    df['diff2'] = df['maxDiff'].rolling(window=3, center=True, min_periods=1).mean()

    # 模型
    clf = Pipeline([('poly', PolynomialFeatures(degree=1)), ('linear', linear_model.LinearRegression())])

    # 训练与预测
    x = df['day'].to_numpy()
    y = df['diff2'].to_numpy()
    clf.fit(x[:, np.newaxis], y)
    x1 = np.arange(x[0], x[-1] + 90)
    y1 = clf.predict(x1[:, np.newaxis])  # 预测值

    # 结果输出
    index = np.where(y1 >= 30)[0]
    if index.size != 0:
        # 若预测值中出现大于30%，则进入此分支
        predict_date = pd.Series((x1 - x[0]) * 86400 * 1e9 + df['timeStamp'].iloc[0] * 1e9)
        predict_date = pd.to_datetime(predict_date, format='%Y-%m-%d')
        return pd.DataFrame([(df['vin'].iloc[0], df['date'].iloc[0], df['date'].iloc[-1], df['maxDiff'].iloc[0],
                              df['maxDiff'].iloc[-1], predict_date.iloc[index[0]].strftime('%Y-%m-%d'),
                              df['maxID'].iloc[-1])])

    elif df['maxDiff'][df['maxDiff'] >= 10].count() > 0 and builtins.round(
            df['maxDiff'].iloc[0]) <= builtins.round(df['maxDiff'].iloc[-1]):
        # 若平均单体偏差最大值大于等于10%，且最近一天的偏差值大于等于最早一天的偏差值，则进入此分支
        return pd.DataFrame([(df['vin'].iloc[0], df['date'].iloc[0], df['date'].iloc[-1], df['maxDiff'].iloc[0],
                              df['maxDiff'].iloc[-1], 'None', df['maxID'].iloc[-1])])

    else:
        # 若不符合上述两个条件，进入此分支
        return pd.DataFrame([(df['vin'].iloc[0], df['date'].iloc[0], df['date'].iloc[-1], 0.0, 0.0, 'None', 0)])


if __name__ == '__main__':

    # 设置SparkSession
    spark = SparkSession.builder.appName('predict_model').enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    spark.conf.set('spark.sql.execution.arrow.enabled', 'true')

    # 输入参数


    if time.strftime("%a", time.localtime()) == 'Mon':
        # 周五才运行此程序
        pt = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")

        warn_date = time.strftime('%Y-%m-%d', time.localtime())

        origin = spark.sql("""
        SELECT * FROM viree_forward_search_dev.fs_dws_temp_carFilter_wi
        WHERE pt = '%s'
        """ % pt)
        origin = origin.withColumn('timeStamp', unix_timestamp(col('date'), 'yyyy-MM-dd').cast('long')).drop('pt')


        # PandasUDF中进行线性回归
        res = origin.groupBy('vin').apply(linear_regression)
        res = res.where(col('cell') != 0).where(col('endTemp') > 10)
        res.persist().count()

        # 储存结果到数仓HIVE表
        res = res.withColumn('longitude', lit('None')) \
            .withColumn('latitude', lit('None')) \
            .withColumn('mileage', lit('None')) \
            .withColumn('warn_kind', lit('48')) \
            .withColumn('warn_date', lit(warn_date)) \
            .withColumn('abnormal_units', concat(lit('U'), col('cell').cast('string'))) \
            .withColumn('endTemp', round(col('endTemp'), 2)) \
            .withColumn('fault_details',
                        when(col('endTemp') >= 29, concat(lit('当前温度偏差='), col('endTemp').cast('string'), lit('℃, 近期已达到30℃')))
                        .when((col('date30') != 'None') & (col('date30') > col('warn_date')) & (col('endTemp') < 29), concat(lit('当前温度偏差='), col('endTemp').cast('string'), lit('℃, 预计于'), col('date30'), lit('达到30℃')))
                        .otherwise(concat(lit('当前温度偏差='), col('endTemp').cast('string'), lit('℃, 有恶化迹象')))) \
            .withColumn('suggestion',
                        when(col('endTemp') >= 29, lit('更换模组，请立刻维修'))
                        .when((col('date30') != 'None') & (col('date30') > col('warn_date')) & (col('endTemp') < 29), lit('更换模组，请在预测日期前维修'))
                        .otherwise(lit('更换模组，请及时维修'))) \
            .withColumn('warn_level',
                        when(col('suggestion') == '更换模组，请立刻维修', lit('3'))
                        .when(col('suggestion') == '更换模组，请在预测日期前维修', lit('2'))
                        .otherwise(lit('1')))
        # res.printSchema()
        # res.show(10, False)

        # 从车辆静态信息表读取车架号-车型-公告号-电池包型号信息
        vin_info = spark.sql("""
        SELECT vin AS v, car_type, declare_type, pkg_type FROM viree_forward_search_dev.fs_dim_vehicle_info_df
        WHERE pt IN (SELECT max(pt) FROM viree_forward_search_dev.fs_dim_vehicle_info_df)
        """)
        res = res.join(vin_info, res['vin'] == vin_info['v'], 'left_outer').where(col('v').isNotNull()).drop('v')
        res.createTempView('predict_result')
        spark.sql("""
        INSERT OVERWRITE TABLE viree_forward_search_dev.fs_dws_temp_consistency_predict_wi_test PARTITION (pt='%s')
        SELECT vin AS vin,
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
        FROM predict_result
        """ % pt)

        # res.repartition(1).write.format('json').mode('append').save('/user/yanjiuy_npdk/trend/' + r'result_temp')

    else:
        print('Today is not Friday.')
