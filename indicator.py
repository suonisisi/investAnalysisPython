import tushare as ts
import pandas as pd
import time

if __name__ == '__main__':

    pd.set_option('display.max_columns', None)

    ts.set_token("cc90ca823298049934eb7a47e5146dba514fabcfca90d16a646265a4")

    pro = ts.pro_api()

    time_range = ['20200331', '20190331', '20191231', '20181231', '20171231', '20161231', '20151231']

    stock_list = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

    indicator_list = ['归属母公司股东的净利润同比增长率(%)',
                      '归属母公司股东的净利润-扣除非经常损益同比增长率(%)',
                      '营业总收入同比增长率(%)','销售毛利率','销售净利率','加权平均净资产收益率']

    first_row = ['','','','','']
    for i in range(len(indicator_list)):
        for j in range(len(time_range)):
            if j == 0:
                first_row.append(indicator_list[i])
            else:
                first_row.append('')

    column_df = pd.DataFrame(columns=first_row)

    column_df.to_csv("./data/医药.csv",mode='a',index=False)


    # stock_list = stock_list[stock_list['industry']=='半导体']
    stock_list = stock_list[(stock_list['industry']=='生物制药')
                            | (stock_list['industry']=='医药商业')
                            | (stock_list['industry']=='医疗保健')
                            | (stock_list['industry']=='化学制药')
                            | (stock_list['industry']=='中成药')
                            ]
    stock_list = stock_list[['ts_code', 'name', 'area', 'industry','list_date']]

    total_count = stock_list.shape[0]


    count = 1

    for index, row in stock_list.iterrows():

        print('共',total_count,'条数据,','第',count,'条数据')
        print(index,row['ts_code'])

        # if index > 0:
        #     break

        print('开始请求')
        indicator = pro.query('fina_indicator', ts_code=row['ts_code'], start_date='20100101', end_date='20200715')
        # cash_flow = pro.cashflow(ts_code=row['ts_code'], start_date='20100101', end_date='20200715')
        print('请求成功')
        # ts_code 代码
        # end_date 	报告期

        # netprofit_margin 销售净利率
        netprofit_margin = indicator[['ts_code', 'end_date', 'netprofit_margin']] \
            .drop_duplicates(['ts_code','end_date'],keep='first')\
            .pivot(index='ts_code', columns='end_date', values='netprofit_margin').reset_index('ts_code')

        # grossprofit_margin 销售毛利率
        grossprofit_margin = indicator[['ts_code', 'end_date', 'grossprofit_margin']] \
            .drop_duplicates(['ts_code','end_date'], keep='first') \
            .pivot(index='ts_code', columns='end_date', values='grossprofit_margin').reset_index('ts_code')

        # roe_waa 加权平均净资产收益率
        roe_waa = indicator[['ts_code', 'end_date', 'roe_waa']] \
            .drop_duplicates(['ts_code','end_date'], keep='first') \
            .pivot(index='ts_code', columns='end_date', values='roe_waa').reset_index('ts_code')

        # netprofit_yoy 归属母公司股东的净利润同比增长率(%)
        netprofit_yoy = indicator[['ts_code', 'end_date', 'netprofit_yoy']] \
            .drop_duplicates(['ts_code','end_date'], keep='first') \
            .pivot(index='ts_code', columns='end_date', values='netprofit_yoy').reset_index('ts_code')

        # dt_netprofit_yoy 归属母公司股东的净利润-扣除非经常损益同比增长率(%)
        dt_netprofit_yoy = indicator[['ts_code', 'end_date', 'dt_netprofit_yoy']] \
            .drop_duplicates(['ts_code','end_date'], keep='first') \
            .pivot(index='ts_code', columns='end_date', values='dt_netprofit_yoy').reset_index('ts_code')

        # tr_yoy 营业总收入同比增长率(%)
        tr_yoy = indicator[['ts_code', 'end_date', 'tr_yoy']] \
            .drop_duplicates(['ts_code','end_date'], keep='first') \
            .pivot(index='ts_code', columns='end_date', values='tr_yoy').reset_index('ts_code')

        for i in range(len(time_range)):


            if time_range[i] not in netprofit_margin.columns.tolist():
                netprofit_margin[time_range[i]] = ''

            if time_range[i] not in grossprofit_margin.columns.tolist():
                grossprofit_margin[time_range[i]] = ''

            if time_range[i] not in roe_waa.columns.tolist():
                roe_waa[time_range[i]] = ''

            if time_range[i] not in netprofit_yoy.columns.tolist():
                netprofit_yoy[time_range[i]] = ''

            if time_range[i] not in dt_netprofit_yoy.columns.tolist():
                dt_netprofit_yoy[time_range[i]] = ''

            if time_range[i] not in tr_yoy.columns.tolist():
                tr_yoy[time_range[i]] = ''

        netprofit_yoy = netprofit_yoy[['ts_code'] + time_range]

        dt_netprofit_yoy = dt_netprofit_yoy[['ts_code'] + time_range]

        tr_yoy = tr_yoy[['ts_code'] + time_range]

        grossprofit_margin = grossprofit_margin[['ts_code'] + time_range]

        netprofit_margin = netprofit_margin[['ts_code'] + time_range]

        roe_waa = roe_waa[['ts_code'] + time_range]


        basic_info = stock_list[stock_list['ts_code']==row['ts_code']]

        indicator_result = netprofit_yoy.merge(dt_netprofit_yoy,on='ts_code',how='left')\
            .merge(tr_yoy,on='ts_code',how='left').merge(grossprofit_margin,on='ts_code',how='left')\
            .merge(netprofit_margin,on='ts_code',how='left').merge(roe_waa,on='ts_code',how='left')

        indicator_result = basic_info.merge(indicator_result,on='ts_code',how='right')

        if count == 1:
            indicator_result.to_csv('./data/医药.csv',index=False,mode='a')
        else:
            indicator_result.to_csv('./data/医药.csv', index=False, mode='a',header=False)

        count = count + 1

        time.sleep(1.3)
