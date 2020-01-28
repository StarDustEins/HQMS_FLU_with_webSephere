import requests
import json
import pymongo
import pandas as pd
import numpy as np
import streamlit as st

data_China = r"https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_global_vars&callback"  # 全球病情总人数
data_City = r"https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_area_datas&callback"  # 地区数据
data_City_V1 = r"https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_area_counts&callback"  # 地区数据


def get_data_text(url):
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        res.encoding = res.apparent_encoding
        return str(res.text)
    except:
        return "Error"


# print(get_data_text(data_China))
# print(get_data_text(data_City))

raw_data = json.loads(json.loads(get_data_text(data_City_V1).replace('\\n', ''))['data'])
# print(raw_data)
mongo = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
cities = mongo['2019nCoV']['city_list']
processed_data = []
for detail in raw_data:
    # print(detail)
    if detail['country'] == '中国':
        city = detail['city']
        area = detail['area']
        if city == '':
            city = detail['area']
        elif city == '外地来京':
            city = '北京'
        city_result = cities.find_one({'city': city})
        city_location = None
        if city_result is None:
            url = 'http://api.map.baidu.com/geocoding/v3/?address={0}&city={2}&output=json&ak={1}'.format(
                city,
                'm7CLmkXFXEMEgOh1lMraLo8vnWagEhP3',
                area)
            # print(url)
            process = json.loads(requests.get(url).text)
            # print(process)
            if process['status'] == 0:
                cities.insert_one({'city': city, 'detail': process['result']})
                # print('web')
                city_location = process['result']['location']
            else:
                pass
        else:
            # print('local')
            city_location = city_result['detail']['location']
        # print(city_location)
        processed_data.append({
            'confirm': detail['confirm'],
            'lng': city_location['lng'],
            'lat': city_location['lat']})
df = pd.DataFrame(
    np.random.randn(1000, 2) / [114, 30],
    columns=['lat', 'lon'])
print(df)
st.deck_gl_chart(layers=[{
    'data': df,
    'type': 'ScatterplotLayer'
}])
