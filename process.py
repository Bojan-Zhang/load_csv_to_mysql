import pandas as pd
import numpy as np
import datetime
import pymysql


def load_csv(csv_file_path):
    # 加载数据
    all_case = pd.read_csv(csv_file_path)
    # 修改不合法的列名
    all_case = all_case.rename(columns={'Unnamed: 5': '年级', 'Unnamed: 6': '题型', 'Unnamed: 7': '学段学科',
                                        'Unnamed: 8': '是否有图表', 'Unnamed: 9': '是否有公式'})
    # 删除无效列名
    all_case = all_case.drop(['书本id', '题目url', '原题', '截图', 'Unnamed: 18',
                              '截图.1', 'Unnamed: 25', 'Unnamed: 26', 'Unnamed: 27', 'Unnamed: 28', 'Unnamed: 29',
                              'Unnamed: 30', 'Unnamed: 31', 'Unnamed: 32', 'Unnamed: 33', 'Unnamed: 34', '0'],
                             axis=1)
    # 删除无效的行
    all_case = all_case.drop(all_case[all_case['题目id'].isna()].index)
    # 删除重复的题
    all_case = all_case.drop_duplicates(['题目id'], 'first')
    # 将题目id转化为int
    all_case['题目id'] = all_case['题目id'].astype(np.int64)
    return all_case


# 规范问题分类的表述：仅有产研、学科、编排这三类问题
def confirm_bug_category(all_case):
    all_case['问题分类'] = all_case['问题分类'].mask(all_case['问题分类'].str.contains('产研', na=False), '产研问题')
    all_case['问题分类'] = all_case['问题分类'].mask(all_case['问题分类'].str.contains('学科', na=False), '学科问题')
    all_case['问题分类'] = all_case['问题分类'].mask(all_case['问题分类'].str.contains('编排', na=False), '编排问题')
    return all_case


# 拆分学科和学段
def split_period_and_subject(all_case):
    all_case['学段'] = all_case['学段学科'].str[0:2]
    all_case['学科'] = all_case['学段学科'].str[2:]
    return all_case


# 拆分是否有图表
def split_pic_and_form(all_case):
    all_case['是否有图'] = '无图'
    all_case['是否有图'] = all_case['是否有图'].mask((all_case['是否有图表'] == '有图'), '有图')
    all_case['是否有表'] = '无表'
    all_case['是否有表'] = all_case['是否有表'].mask((all_case['是否有图表'] == '有表格'), '有表')
    return all_case


# 添加更新时间
def add_time_stamp(all_case):
    all_case['更新时间'] = np.datetime64(datetime.datetime.now())
    return all_case


# 与mysql数据库建立连接
def get_mysql_conn(host, port, user, pwd, db_name, charset="utf8"):
    try:
        db = pymysql.connect(host=host,
                             port=port,
                             user=user,
                             password=pwd,
                             db=db_name,
                             charset=charset)
        print("数据库连接成功")
        return db
    except Exception as e:
        print("数据库连接失败", e)


# 对Dataframe进行类型转换
def make_table_sql(df):
    # 获取df中的列名以及列的数据格式
    columns = df.columns.tolist()
    types = df.dtypes
    # 按照列的数据格式来确定sql表的数据格式
    table = []
    field = []
    for item in range(len(columns)):
        if 'object' == str(types[item]):
            column_type = '`' + columns[item] + '`' + ' VARCHAR(255)'  # 必须加上`这个点，否则在写入mysql是会报错
        elif 'int64' == str(types[item]):
            column_type = '`' + columns[item] + '`' + ' INT'
        elif 'float64' == str(types[item]):
            column_type = '`' + columns[item] + '`' + ' FLOAT'
        elif 'datetime64[ns]' == str(types[item]):
            column_type = '`' + columns[item] + '`' + ' DATETIME'
        else:
            column_type = '`' + columns[item] + '`' + ' VARCHAR(255)'
        table.append(column_type)
        field.append('`' + columns[item] + '`')
    return ','.join(table), ','.join(field)


# 将解析后的csv文件（Dataframe类型）的数据导入mysql
def write_csv_to_mysql(df, conn, file_name):
    # 转换数据格式
    tables, fields = make_table_sql(df)
    # 连接数据库，开始操作数据库
    try:
        # 获取数据库光标
        cur = conn.cursor()
        # 如果表已经存在则删除
        cur.execute("drop table if exists {};".format(file_name))
        conn.commit()
        # 创建表格，并设置表格的列文字，以及列的数据格式
        print('开始创建表：' + file_name)
        table_sql = 'CREATE TABLE IF NOT EXISTS ' + file_name + \
                    '(' + 'id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,' + tables + ');'
        cur.execute(table_sql)
        conn.commit()
        print('表：' + file_name + "创建成功")
        # 写入数据
        print('开始写入数据，表：' + file_name)
        # 将原来从csv文件获取得到的空值数据设置成None，不设置将会报错
        df_sql = df.astype(object).where(pd.notnull(df), None)
        # 获取数值
        values = df_sql.values.tolist()
        # 获得文件数据有多少列，每个列用一个 %s 替代
        s = ','.join(['%s' for _ in range(len(df.columns))])
        # 写入数据
        insert_sql = 'insert into {}({}) values({})'.format(file_name, fields, s)
        cur.executemany(insert_sql, values)
        conn.commit()
        print("表：" + file_name + "数据写入完成")
    except Exception as e:
        print("写入失败", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    data = load_csv("D:\DBIS\腾讯课题实践项目\置信学习应用检测标注错误\Data\题目质量-工作表1-0316.csv")
    data = confirm_bug_category(data)
    data = split_period_and_subject(data)
    data = split_pic_and_form(data)
    data = add_time_stamp(data)
    db_conn = get_mysql_conn("192.168.190.106", 3306, "root", "123456", "case_data")
    write_csv_to_mysql(data, db_conn, "题目质量0316")
