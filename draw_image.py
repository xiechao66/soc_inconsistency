from datetime import datetime
import os
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from pylab import mpl

mpl.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题


def draw_image(df: pd.DataFrame):
    df = df.sort_values(by=['date', 'startMileage'])
    vin = df["Vin"].iloc[0]
    # car_type = car_info[car_info["vin"] == vin]["car_type"].iloc[0]
    # pkg_type = car_info[car_info["vin"] == vin]["pkg_type"].iloc[0].split('_')[0]
    # warn_flag = car_info[car_info["vin"] == vin]["warn_flag"].iloc[0]

    date = df["date"]
    date = [datetime.strptime(str(d), '%Y-%m-%d').date() for d in date]
    mileage = df["startMileage"]
    selected_columns = []
    for i in range(num):  # 假设你要选择的列数为n
        column_name = 'U' + str(i + 1)
        selected_columns.append(column_name)
    avg = df.loc[:, selected_columns]
    a = np.where(np.max(avg, ) > 5)
    fig1 = plt.figure(figsize=(15, 8), dpi=300)  # 确定画布大小
    ax1 = fig1.add_subplot(2, 1, 1)  # 绘制第1幅子图

    color = ['#ff7f0e', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
    ax1.plot(mileage, avg, label=avg.columns, linewidth=1, color='steelblue')
    if np.size(a) != 0:
        for i in range(np.size(a)):
                if i <= 7:
                    ax1.plot(mileage, avg.iloc[:, int(a[0][i] + 1) - 1], 'o-', label="离群单体U" + str(a[0][i] + 1), linewidth=1.5,
                             color=color[i])
                else:
                    ax1.plot(mileage, avg.iloc[:, int(a[0][i] + 1) - 1], 'o-', label="离群单体U" + str(a[0][i] + 1), linewidth=1.5)
    else:
        diff = np.sum(abs(avg), axis=0)/len(avg)
        a1 = diff.idxmax()
        print(a1)
        ax1.plot(mileage, avg.loc[:, str(a1)], 'o-', label="离群单体U" + str(a1).split("U")[1], linewidth=1.5,
                 color=color[0])

    # plt.legend(bbox_to_anchor=(1, 0), loc=3, borderaxespad=0, fontsize=3, ncol=3)#
    fig1.subplots_adjust(top=0.95, bottom=0.1, right=0.8, left=0.05)
    ax1.set_xlim([mileage.iloc[0], mileage.iloc[-1]])
    ax1.grid(True, linestyle='--', alpha=0.5)
    ax1.set_ylabel('ΔSOC(%)', fontsize=15)
    # ax1.set_xlabel('里程(km)', fontsize=15)
    # ax1.xaxis.set_label_coords(0.5, 5.1)
    ax1.tick_params(axis='both', labelsize=13)
    plt.title(f"{vin}", fontsize=18)
    # plt.show()

    ax2 = fig1.add_subplot(2, 1, 2)  # 绘制第1幅子图

    color = ['#ff7f0e', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
    ax2.plot(date, avg, label=avg.columns, linewidth=1, color='steelblue')
    if np.size(a) != 0:
        for i in range(np.size(a)):
            if i <= 7:
                ax2.plot(date, avg.iloc[:, int(a[0][i] + 1) - 1], 'o-', label="离群单体U" + str(a[0][i] + 1), linewidth=1.5,
                         color=color[i])

            else:
                ax2.plot(date, avg.iloc[:, int(a[0][i] + 1) - 1], 'o-', label="离群单体U" + str(a[0][i] + 1), linewidth=1.5)
    else:
        diff = np.sum(abs(avg), axis=0)/len(avg)
        a1 = diff.idxmax()

        ax2.plot(date, avg.loc[:, str(a1)], 'o-', label="离群单体U" + str(a1).split("U")[1], linewidth=1.5,
                 color=color[0])

    plt.legend(bbox_to_anchor=(1, 0), loc=3, borderaxespad=0, fontsize=9, ncol=3)  #
    # fig1.subplots_adjust(top=0.95, bottom=0.1, right=0.8, left=0.05)
    ax2.grid(True, linestyle='--', alpha=0.5)

     # 设置日期格式
    ax2.tick_params(axis='both', labelsize=13)
    ax2.set_xticklabels(ax2.get_xticks(), rotation=45)

    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d'))
    ax2.set_xlim([date[0], date[-1]])
    # 设置主要刻度定位器为自动选择
    ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax2.set_ylabel('ΔSOC(%)', fontsize=15)
    # plt.title(f"{vin}")
    # 调整子图之间的垂直间距
    plt.subplots_adjust(hspace=0.15)
    # plt.show()
    os.makedirs(inputpath + f"\\SOC图\\", exist_ok=True)
    plt.savefig(inputpath + "\\SOC图\\" + f"{vin}.png.", dpi=300, format="png")

    # os.makedirs(inputpath + f"\\里程_SOC\\{warn_flag}\\{car_type}\\{pkg_type}\\", exist_ok=True)
    # plt.savefig(inputpath + f"\\里程_SOC\\{warn_flag}\\{car_type}\\{pkg_type}\\" + f"{vin}.png.", dpi=300, format="png")


if __name__ == '__main__':
    num = 96
    inputpath = Rf'E:\单体SOC不一致\数据\L6T79T2E2PP035818\\'
    # df = pd.read_excel(inputpath + '\soc_feature0908.xlsx')
    # df = pd.read_csv(inputpath + 'soc_feature2.csv')
    df = pd.read_json(inputpath + "L6T79T2E2PP035818.json", lines=True, convert_dates=False)

    # df = df[df['vin']=='HESCA2C49PS000054']
    # car_info = pd.read_csv(inputpath + "\\warn.csv")
    # car_info["ΔSOC"] = car_info["fault_details"].str.split('=').str.get(1)
    # car_info.loc[:, ['vin','product_date','battery_pack_code','battery_pack_prd_time','warn_level','abnormal_units',
    #                  'start_time','end_time','suggestion','mileage','fault_details','ΔSOC','car_type','declare_type','pkg_type',
    #                  'mf_date','sale_time','delivery_confirm_time','pt','count','warn_flag'
    #                  ]].to_excel(inputpath + f'\\soc不一致预警结果明细_{date}.xlsx', index=False)

    df = df.groupby("Vin").apply(draw_image)
