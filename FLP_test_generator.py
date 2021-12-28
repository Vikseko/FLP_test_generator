#!/usr/bin/env python3.8
import time
import sys
import random
import argparse
import math
import os
from functools import reduce
from threading import Timer
import functools
print = functools.partial(print, flush=True)
from itertools import combinations
import itertools
import copy
import signal

#Parser
def createParser ():
    parser = argparse.ArgumentParser()
    #python3.8 FLP_test_generator.py -s 357 -c 1418601 -x 50 -y 50 -b 1307449 -csv 1 -r 2-3 -n 1
    parser.add_argument ('-s', '--nof_stations', nargs='?', type=int, default=120, help='Число базовых станций')
    parser.add_argument ('-c', '--nof_clients', nargs='?', type=int, default=1000000, help='Число клиентов')
    parser.add_argument ('-x', '--max_x', nargs='?', type=int, default=50, help='Максимум по оси X для сетки')
    parser.add_argument ('-y', '--max_y', nargs='?', type=int, default=50, help='Максимум по оси Y для сетки')
    parser.add_argument ('-r', '--radius', nargs='?', type=str, default='2-4', help='Диапазон радиуса для станцией (через дефис, для каждой станции будет выбрано случайное значение из диапазона)')
    parser.add_argument ('-b', '--bound', nargs='?', type=int, default=700000, help='Минимальное число обслуживаемых клиентов (Граница для весов)')
    parser.add_argument ('-csv', '--csv_output_flag', nargs='?', type=int, default=0, help='Делать ли вывод в .csv формате, помимо CNF')
    parser.add_argument ('-wcnf', '--wcnf_output_flag', nargs='?', type=int, default=0, help='Делать ли вывод в .wcnf формате, помимо CNF')
    parser.add_argument ('-n', '--number_of_tests', nargs='?', type=int, default=1, help='Сколько тестов делать')
    parser.add_argument ('-stdict', '--station_dictionary_flag', nargs='?', type=int, default=0, help='Делать ли вывод станций вместе с их количеством клиентов в отдельный файл')
    return parser

def create_station(number,max_x,max_y,radius):
    coord_x = random.randint(0,max_x-1)
    coord_y = random.randint(0,max_y-1)
    rad1 = int(radius.split('-')[0])
    rad2 = int(radius.split('-')[1])
    radius = random.uniform(rad1,rad2)
    station = (number,coord_x,coord_y,radius)
    return station

def create_client(number,max_x,max_y):
    coord_x = random.uniform(0,max_x-1)
    coord_y = random.uniform(0,max_y-1)
    client = (number,coord_x,coord_y)
    return client

def create_stations(nof_stations,max_x,max_y,radius):
    list_of_stations = []
    set_stations_ = set()
    i = 0
    while len(list_of_stations) < nof_stations:
        new_station = create_station(i,max_x,max_y,radius)
        if (new_station[1],new_station[2]) in set_stations_:
            continue
        else:
            list_of_stations.append(new_station)
            set_stations_.add((new_station[1],new_station[2]))
            i += 1
    return list_of_stations

def create_clients(nof_clients,max_x,max_y):
    list_of_clients = []
    for i in range(nof_clients):
        list_of_clients.append(create_client(i,max_x,max_y))
    return list_of_clients

def compute_distance(station, client):
    dx = station[1] - client[1]
    dy = station[2] - client[2]
    return math.sqrt(dx*dx + dy*dy)


def clauses_dict(clauses_list_with_duplicates):
    clauses_dictinary = {}
    for clause in clauses_list_with_duplicates:
        if clause not in clauses_dictinary:
            clauses_dictinary[clause] = 1
        else:
            clauses_dictinary[clause] += 1
    return clauses_dictinary


def create_csv_list(list_of_clients, list_of_stations,nof_stations,nof_clients,min_weights_sum):
    csv_result = []
    first_string = str(nof_stations) + ',' + str(nof_clients) + ',' + str(min_weights_sum)
    csv_result.append(first_string)
    #print(nof_stations,nof_clients,min_weights_sum,sep=',', file = out_csv_file)
    for client in list_of_clients:
        nof_stations_for_that_client = 0
        list_of_stations_for_that_client = []
        for station in list_of_stations:
            radius = station[3]
            station_number = station[0]
            distance = compute_distance(station,client)
            if distance <= radius:
                nof_stations_for_that_client += 1
                list_of_stations_for_that_client.append(station_number)
        client_string = str(nof_stations_for_that_client)
        for station in list_of_stations_for_that_client:
            client_string += ',' + str(station)
        csv_result.append(client_string)
    return csv_result


def create_cnf_clauses(list_of_clients, list_of_stations):
    clauses_dictinary = {}
    nof_zeros = 0
    for client in list_of_clients:
        nof_stations_for_that_client = 0
        list_of_stations_for_that_client = []
        for station in list_of_stations:
            radius = station[3]
            station_number = station[0]
            distance = compute_distance(station,client)
            if distance <= radius:
                nof_stations_for_that_client += 1
                list_of_stations_for_that_client.append(station_number)
        clause = ''
        for station in list_of_stations_for_that_client:
            clause += str(station+1) + ' '
        clause += '0'
        if clause == '0':
            nof_zeros += 1
        elif clause not in clauses_dictinary:
            clauses_dictinary[clause] = 1
        else:
            clauses_dictinary[clause] += 1
    return nof_zeros, clauses_dictinary


def create_stations_dict(list_of_clients, list_of_stations):
    stations_dictionary = {}
    for station in list_of_stations:
        radius = station[3]
        station_number = station[0]+1
        for client in list_of_clients:
            distance = compute_distance(station,client)
            client_number = client[0]+1
            if distance <= radius:
                if station_number not in stations_dictionary:
                    stations_dictionary[station_number] = str(client_number)
                else:
                    stations_dictionary[station_number] += ' ' + str(client_number)
    return stations_dictionary

def stations_visualization(list_of_stations,max_x,max_y):
    visual_map = [[0] * max_x for i in range(max_y)]
    #print(*visual_map,sep='\n')
    for station in list_of_stations:
        coord_x = station[1]
        coord_y = station[2]
        #print(coord_x,coord_y)
        visual_map[coord_y-1][coord_x-1] = 1
    print(*visual_map,sep='\n')

def sum_of_values(stations_dictionary):
    sum_values = 0
    for key in stations_dictionary:
        sum_values += stations_dictionary[key]
    return sum_values

#============================================================================#

#Параметры
start_time = time.time()
parser = createParser()
namespace = parser.parse_args (sys.argv[1:])
nof_stations = namespace.nof_stations
nof_clients = namespace.nof_clients
max_x = namespace.max_x
max_y = namespace.max_y
radius = namespace.radius
min_weights_sum = namespace.bound
csv_flag = namespace.csv_output_flag
stdict_flag = namespace.station_dictionary_flag
nof_tests = namespace.number_of_tests
wcnf_flag = namespace.wcnf_output_flag

counter = 0
while counter < nof_tests:
    start_test_time = time.time()
    out_name = 'test_' + str(counter) + '_' + str(nof_stations)+'_'+str(nof_clients)+'_'+str(min_weights_sum)+'_radius'+radius
    out_name_cnf = './CNFs/' + out_name + '.cnf'
    out_name_stations = './Stations/' + out_name + '.stations'
    out_name_csv = './CSVs/' + out_name + '.csv'
    out_name_wcnf = './WCNFs/' + out_name + '.wcnf'

    #Создаём станции и клиентов в случайных местах сетки
    list_of_stations = create_stations(nof_stations,max_x,max_y,radius)
    #stations_visualization(list_of_stations,max_x,max_y)
    list_of_clients = create_clients(nof_clients,max_x,max_y)

    #создаем дизъюнкты, считая расстояние
    nof_zeros, clauses_dictinary  = create_cnf_clauses(list_of_clients, list_of_stations)

    #Создаем выходную КНФ
    header = 'p cnf ' + str(nof_stations) + ' ' + str(len(clauses_dictinary))
    core_vars = 'c core vars'
    for i in range(1,nof_stations+1):
        core_vars += ' ' + str(i)
    clauses_weights = 'c clauses weight'
    for key in clauses_dictinary:
        clauses_weights += ' ' + str(clauses_dictinary[key])
    #sum_clauses_weights = sum([int(x) for x in clauses_weights.split()[3:]])
    if nof_clients - nof_zeros < min_weights_sum:
        print('ERROR Sum of weights is too low:',nof_clients - nof_zeros,'/',min_weights_sum)
        continue
    with open(out_name_cnf,'w') as out_file:
        print(header, file=out_file)
        print(core_vars, file=out_file)
        print('c number of zeros',nof_zeros, file=out_file)
        print(clauses_weights, file=out_file)
        print('c min weights sum',min_weights_sum, file=out_file)
        print(*clauses_dictinary.keys(),sep='\n', file=out_file)
        #если нужен вывод в формате "('clause', weight)"
        #print(*clauses_dictinary.items(),sep='\n', file=out_file)
    print('Test', counter, 'CNF done, total runtime =',time.time()-start_test_time)

    if wcnf_flag == 1:
        comment = 'c ' + str(nof_stations) + ' ' + str(nof_clients) + ' ' + str(min_weights_sum)
        header = 'p cnf ' + str(nof_stations) + ' ' + str(len(clauses_dictinary)) + ' ' + str(sum_of_values(clauses_dictinary))
        #sum_clauses_weights = sum([int(x) for x in clauses_weights.split()[3:]])
        with open(out_name_wcnf,'w') as out_file:
            print(comment, file=out_file)
            print(header, file=out_file)
            for key in clauses_dictinary:
                print(clauses_dictinary[key],key,file=out_file)
            #если нужен вывод в формате "('clause', weight)"
            #print(*clauses_dictinary.items(),sep='\n', file=out_file)
        print('Test', counter, 'WCNF done, total runtime =',time.time()-start_test_time)

    #Создаем файл в котором список станций с их клиентами
    if stdict_flag != 0:
        stations_dictionary = create_stations_dict(list_of_clients, list_of_stations)
        with open(out_name_stations,'w') as out_file:
            #если нужен вывод в формате "('station', clients)"
            print(*stations_dictionary.items(),sep='\n', file=out_file)
        print('Test', counter, 'stations dictionary done, total runtime =',time.time()-start_test_time)

    #Тут функция, которая делает csv файл, но она его делает заново
    #(ну в смысле параметры клиентов и станций такие же, но расстояние и все такое считает заново)
    #так что её включение вдвоё увеличивает время работы
    if csv_flag != 0:
        csv_result = create_csv_list(list_of_clients, list_of_stations,nof_stations,nof_clients,min_weights_sum)
        with open(out_name_csv,'w') as out_csv_file:
            print(*csv_result,sep='\n',file=out_csv_file)
        print('Test', counter, 'CSV done, total runtime =',time.time()-start_test_time)
    counter += 1

print(nof_tests, 'tests created, total runtime', time.time()-start_time)