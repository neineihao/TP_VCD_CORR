import sqlite3
import os.path

class SQL_Manager():
    def __init__(self,filename):
        self.file = filename

    def __enter__(self):
        print("Open the database: {}".format(self.file))
        self.conn = sqlite3.connect(self.file)
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Exit the database : {}".format(self.file))
        self.cursor.close()
        self.conn.commit()
        self.conn.close()

def compare_path(t_list, origin, time_dict):
    check_list=[]
    compare_list=[]
    check_dit = {}
    count = 0
    print(t_list)
    if len(t_list) == 1:
        return t_list[0]
    else:
        for i,item in enumerate(t_list):
            key = item[2].split('/')[1]
            if key in check_dit.keys():
                if check_list[check_dit[key]][0] < item[0]:
                    check_list[check_dit[key]] = item
            else:
                check_dit[key] = count
                check_list.append(item)
                count += 1
        if len(check_list) == 1:
            return check_list[0]
        else:
            print("check_list now : {}".format(check_list))
            print("will compare to : {}".format(origin))
            o_t = origin[0]
            o_l = origin[2].split('/')
            o_e = o_l[0]
            o_s = o_l[1]
            for item in check_list:
                c_s = item[2].split('/')[1]
                key = "{}{}{}{}{}".format(o_e, c_s,item[1], o_s, origin[1])
                print("The key : {}".format(key))
                print("The value we get: {}".format(time_dict[key]))
                v_g = time_dict[key]
                print("The time we get: {}".format(origin[0]-item[0]))
                t_g = origin[0] - item[0]
                if abs(t_g - v_g) < 0.00001:
                    return item
            input("stop here")
            return (check_list[0])

def read_sdf(filename='./precom_sdf.txt'):
    time_dict = {}
    with open(filename) as f:
        for line in f:
            line.strip()
            i_l = line.split()
            # print(i_l)
            key = "{}{}{}{}{}".format(i_l[0], i_l[1], i_l[2], i_l[3], i_l[4])
            time_dict[key] = float(i_l[5])
            # print("key : {}, value: {}".format(key, float(i_l[5])))
    return time_dict

def find_which(o_tuple,list_tuple, sdf_dic):
    result_item = []
    for item in list_tuple:
        if item[0] - o_tuple[0] == sdf_dic[item[1]]:
            result_item.append(item[0])
    if result_item > 1:
        raise ValueError("More than two path match")
    return result_item[0]


def sql_build():
    with SQL_Manager('Data.db') as sql:
        sql.execute('''CREATE TABLE IF NOT EXISTS gate_data
( 
  ID  INTEGER PRIMARY KEY,
  work_time float NOT NULL,
  State int NOT NULL,
  From_gate varchar(50) NOT NULL,
  To_gate varchar(50) NOT NULL
)
''')
        list_data = read_data()
        for d_i in list_data:
            sql.execute('''INSERT INTO gate_data (work_time, State, From_gate, To_gate)
        VALUES (?, ?, ?, ?)''',(d_i['time'], d_i['state'], d_i['From'], d_i['To']))


def search_from_to(to_name, sql):
    sql.execute('''
    SELECT work_time, State, From_gate From gate_data as gd
    WHERE gd.To_gate=?
''', (to_name,))
    return sql.fetchall()

def main():
    s_l = []
    time_dict = read_sdf()
    with SQL_Manager('Data.db') as sql:
        s_r = search_from_to('mu0_z_reg_11_/D', sql)
        s_l.append(s_r)
        while True:
            if s_r:
                c_r = compare_path(s_r, s_l[-1], time_dict)
                s_l.append(c_r)
                print("s_l: {}".format(s_l))
                s_r = search_from_to(c_r[2], sql)

            else:
                print("end searching !!")
                break

def read_data():
    item_list = []
    with open('./read_file') as rf:
        for line in rf:
            line.strip()
            bulk = line.split()
            if len(bulk)  > 3:
                # print(bulk)
                tmp_d = {'time': int(bulk[0])/1000 , 'state': bulk[3], 'From': bulk[5].split('.')[2],
                         'To': bulk[7].split('.')[2]}
                item_list.append(tmp_d)
    return item_list



if __name__ == '__main__':
    # r_l = read_data()
    # for item in read_data():
    #     for key, value in item.items():
    #         print("key : {}, value: {}".format(key, value) )

    if os.path.isfile("./Data.db") == False:
        sql_build()
    main()
    # read_sdf()