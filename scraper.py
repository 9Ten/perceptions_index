# -*- coding: utf-8 -*-
''' Stat Total [30421]
    area_id [226], province_id [77]
    {
        'สปป': 183, 
        'สปม': 42, 
        'พิเศษ': 1
    }
'''
#=== Import Library ===#
# For web scraping
from bs4 import BeautifulSoup
import requests
import re
import datetime
import pprint

# For data structure
from collections import OrderedDict
import multiprocessing
import pandas as pd
import os

# Dev Chrome
pp = pprint.PrettyPrinter(indent=4)
header = {'User-Agent': 'Chrome/61.0.3163.100'}
base_url = "http://data.bopp-obec.info/emis/"
main_url = "http://data.bopp-obec.info/emis/area_school.php"


# ข้อมูลพื้นฐาน
info_columns = [
    'รหัสโรงเรียน 10 หลัก',
    'รหัส Smis 8 หลัก',
    'รหัส Obec 6 หลัก',
    'ชื่อสถานศึกษา(ไทย)',
    'ชื่อสถานศึกษา(อังกฤษ)',
    'ที่อยู่',
    'ตำบล',
    'อำเภอ',
    'จังหวัด',
    'รหัสไปรษณีย์',
    'โทรศัพท์',
    'โทรสาร',
    'ระดับที่เปิดสอน',
    'วัน-เดือน-ปี ก่อตั้ง',
    'อีเมล์',
    'เว็บไซต์',
    'เครือข่ายพัฒนาคุณภาพการศึกษา',
    'องค์กรปกครองส่วนท้องถิ่น',
    'ระยะทางจากโรงเรียน ถึง เขตพื้นที่การศึกษา',
    'ระยะทางจากโรงเรียน ถึง อำเภอ',
]

# ข้อมูลคอมพิวเตอร์และระบบเครือข่ายอินเทอร์เน็ต
com_internet_columns = [
    'การเรียนการสอน_งบประมาณ สพฐ.',
    'การเรียนการสอน_จัดหาเอง/บริจาค',
    'การเรียนการสอน_รวม',
    'การเรียนการสอน_ใช้งานได้',
    'การเรียนการสอน_ใช้งานไม่ได้',
    'การบริหารจัดการ_งบประมาณ สพฐ.',
    'การบริหารจัดการ_จัดหาเอง/บริจาค',
    'การบริหารจัดการ_รวม',
    'การบริหารจัดการ_ใช้งานได้',
    'การบริหารจัดการ_ใช้งานไม่ได้',
    'คอมพิวเตอร์',
    'ใช้งานได้',
    'ใช้งานไม่ได้',
    'ข้อมูลคอมพิวเตอร์ ณ วันที่',
    'MOEnet_ผู้ให้บริการ',
    'MOEnet_ประเภท',
    'MOEnet_ความเร็ว',
    'MOEnet_สถานะการใช้งาน',
    'โรงเรียนเช่าเอง_ผู้ให้บริการ',
    'โรงเรียนเช่าเอง_ประเภท',
    'โรงเรียนเช่าเอง_ความเร็ว',
    'โรงเรียนเช่าเอง_สถานะการใช้งาน',
    'โรงเรียนเช่าเอง_งบประมาณ/เดือน',
    'ระบบเครือข่าย (LAN)',
    'ระบบเครือข่ายไร้สาย (Wireless LAN)',
    'ระบบเครือข่ายอินเทอร์เน็ต ณ วันที่',
]

# ข้อมูลครู/บุคลากร
teacher_columns = [
    'ชื่อ',
    'สกุล',
    'ตำแหน่ง-วิทยฐานะ',
    'รูป'
]


# get url
def retry(url):
    while True:
        try:
            res = requests.get(url, headers=header, timeout=10)
            if res.status_code == 200:
                break
        # except requests.exceptions.ReadTimeout:
        # except requests.exceptions.ConnectTimeout:
        except Exception as e:
            print("===== RETRY =====")
            print(e)
            print("Continue ...")
            continue
    return res


#=== Extract Info ===#
def info_tocsv(school, area_code, area_name):
    # school_ID = re.search(r'School_ID=[0-9]+', school['value']).group(0)
    url = base_url + school['value']
    res_info = retry(url)
    try:
        info_table = BeautifulSoup(res_info.content, 'lxml').find(
            'table', {'width': "521", 'align': 'center'})
    except Exception as e:
        print('{}: ERROR {}'.format(url, e))
    info_list = info_table.find_all('div', {'align': 'left'})
    date = info_table.find('div', {'align': 'center'})

    csv_dict = OrderedDict()
    csv_dict['รหัสเขต'] = area_code
    csv_dict['เขตพื้นที่'] = area_name
    for col, row in zip(info_columns, info_list):
        if row.a != None:
            # website
            site_list = [a['href'] for a in row.find_all('a')]
            site_list[1] = "http://data.bopp-obec.info" + \
                site_list[1][2:]
            row = ','.join(site_list)
        else:
            row = re.sub('[\s]', ' ', row.text.strip())
        csv_dict[col] = row

    # Tranform into Datetime
    csv_dict['ข้อมูล ณ วันที่'] = date.text.strip()[17:-1]
    # pp.pprint(csv_dict)
    df = pd.DataFrame([csv_dict])
    df.to_csv('info.csv', mode='a', index=False,
              encoding="utf-8", header=(not os.path.exists('info.csv')))


#=== Extract com_internet ===#
def com_internet_tocsv(school, school_id, area_code):
    url = base_url + 'schooldata-view_com-internet.php?' + \
        school_id + '&Area_CODE=' + area_code
    res_com_internet = retry(url)
    try:
        com_internet_table = BeautifulSoup(
            res_com_internet.content, 'lxml').find_all('tr', {'valign': 'baseline'})
    except Exception as e:
        print('{}: ERROR {}'.format(url, e))

    com_internet_list = []
    append = com_internet_list.append
    for com_internet in com_internet_table:
        try:
            if com_internet.find('div', {'align': 'left'}).img is None:
                com_internet = com_internet.find(
                    'div', {'align': 'left'}).text.strip().replace(' เครื่อง', '')
            else:
                continue
        except:
            # datetime update
            try:
                com_internet = re.search(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', com_internet.text).group(0)
            except:
                com_internet = ""
        append(com_internet)

    csv_dict = OrderedDict()
    csv_dict['รหัสเขต'] = area_code
    csv_dict['รหัสโรงเรียน 10 หลัก'] = school_id[10:]
    for col, row in zip(com_internet_columns, com_internet_list):
        csv_dict[col] = row

    # pp.pprint(csv_dict)
    df = pd.DataFrame([csv_dict])
    df.to_csv('com_internet.csv', mode='a', index=False,
              encoding="utf-8", header=(not os.path.exists('com_internet.csv')))


#=== Extract teacher ===#
def teacher_tocsv(school, school_id, area_code):
    url = base_url + 'schooldata-view_techer.php?' + \
        school_id + '&Area_CODE=' + area_code
    res_teacher = retry(url)
    try:
        teacher_table = BeautifulSoup(
            res_teacher.content, 'lxml').find('div', class_="style85")
        # detail_list
        detail_list = teacher_table.find_all('span', class_="style65")
        # image_list
        image_list = teacher_table.find_all('img')
    except Exception as e:
        print('{}: ERROR {}'.format(url, e))

    teacher_list = []
    append = teacher_list.append
    for teacher_detail, teacher_img in zip(detail_list, image_list):
        csv_dict = OrderedDict()
        csv_dict['รหัสเขต'] = area_code
        csv_dict['รหัสโรงเรียน 10 หลัก'] = school_id[10:]
        name = list(map(lambda name: re.sub(
            '[\s]', ' ', name.text.strip()), teacher_detail.find_all('a')))
        # fname = list(filter(lambda n: n != "", name[0].split(" ")))
        try:
            # csv_dict['ชื่อ'] = fname[0]
            # csv_dict['สกุล'] = fname[1]
            csv_dict['ชื่อ-สกุล'] = name[0]
            csv_dict['ตำแหน่ง-วิทยฐานะ'] = name[1]
        # Bug full-name
        except Exception as e:
            print('{}: ERROR {}'.format(url, e))
            continue

        csv_dict['รูป'] = base_url + teacher_img['src']
        teacher_list.append(csv_dict)
    df = pd.DataFrame(teacher_list)
    df.to_csv('teacher.csv', mode='a', index=False,
              encoding="utf-8", header=(not os.path.exists('teacher.csv')))


#=== Process 3 CSV file ===#
def etl_tocsv(area):
    res = retry(base_url + area[0])
    area_code = re.search(r'[0-9]+', area[0]).group(0)
    area_name = area[1]
    school_list = BeautifulSoup(res.content, 'lxml').find_all('option')
    # School_ID
    for school in school_list[1:]:
        school_id = re.search(r'School_ID=[0-9]+', school['value']).group(0)
        info_tocsv(school, area_code, area_name)
        com_internet_tocsv(school, school_id, area_code)
        teacher_tocsv(school, school_id, area_code)


if __name__ == '__main__':
    start = datetime.datetime.now()
    print("Time Start: {}".format(start))
    # Extract Area_CODE
    res = requests.get(main_url, timeout=10)
    soup = BeautifulSoup(res.content, 'lxml').find_all('a')
    # Area_CODE tuple (key: value)
    area_list = list(map(lambda d: (d['href'], d.text), soup[2:-1]))

    #=== Parallel (multiprocessing) ===#
    # pool = multiprocessing.Pool(4)
    # pool.map(etl_tocsv, area_list, chunksize=4)

    #=== Serial Processing ===#
    for area in list(area_list):
        etl_tocsv(area)
    end = datetime.datetime.now()
    print("Time End: {}".format(end))
    print("Time Cost: {}".format(end - start))

'''
Time Cost: 1:37:03.554663
'''
