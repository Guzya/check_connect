"""
Проверяем подключение к БД.
Помогает оценить время не доступности БД.

Скрипт устанавливает соединение с БД,
создает таблицу guzya_"%Y_%m_%d_%H_%M" (int, timestamp),
начинает писать в таблицу.
При обрыве соединения скрипт пытается переподключается с таймоутом ожидания 1 сек,
если коннект не получен, увеличивается счетчик ошибок, увеличивается счетчик строк
и повторяем попытку подключиться.

ОСТАНОВКА СКРИПТА:
Ctrl + c

При завершении работы скрипт выводит:
- количество успешных соединений с базой,
- количество ошибок при работе с базой,
- счетчик строк,
- кол. переданных строк,
- кол. записанных строк,
- мин. задержку м\у соседними строками,
- макс. задержку м\у соседними строками,
- сред. задержку м\у соседними строками

По умолчанию, созданная таблица удаляется.

Запуск скрипта:

python3 check_connect.py --h 172.1.1.228 --p 5432 --d postgres --U postgres --P postgres

python3 check_connect.py --h 172.1.1.228 --p 5432 --d postgres --U postgres --P postgres --C no

"""

import datetime
import os
import logging
import argparse
import psycopg2 as pg

def get_connect(db_name,db_user,db_password,db_host,db_port):
    '''Соединение с бд'''
    conn = pg.connect(dbname=db_name, user=db_user,password=db_password, host=db_host,port=db_port,connect_timeout=1)
    return conn


def main(db_name,db_user,db_password,db_host,db_port,clear):
    
    
    logger.info('{} : {} : {} : {} : {}'.format(db_host ,db_port ,db_name ,db_user ,'******'))
    
    db_table = 'guzya_{}'.format(datetime.datetime.now().strftime("%Y_%m_%d_%H_%M"))
    logger.info('Приступаю к созданию таблицы.')
    try:
        conn = get_connect(db_name,db_user,db_password,db_host,db_port)
        logger.info('Получил соединение с БД.')
        cur = conn.cursor()
        logger.info('Получил курсор.')
        cur.execute("CREATE TABLE {} (id int, timest timestamp);".format(db_table))       
        cur.connection.commit()
        cur.close()
        conn.close()
        logger.info('Выполнил создание таблицы.')
                        
    except pg.Error as e:
            logger.error(e)
            logger.error('Не прошла инициализация, завершаем работу.')
            
            return
                
    counter_row = 0
    counter_conn = 0
    counter_conn_err = 0
    is_connected = False
    
    logger.info('Захожу в цикл проверки...')

    while True:
        try:
            counter_row = counter_row + 1
            logger.info('{}'.format(counter_row ))
            if not is_connected:
                conn = get_connect(db_name,db_user,db_password,db_host,db_port)
                is_connected = True
                cur = conn.cursor()
                counter_conn = counter_conn + 1            
            cur.execute("insert into {} (id, timest) values({},'{}');".format(db_table,counter_row,datetime.datetime.now()))
            cur.connection.commit()
           # cur.close()
           # conn.close()
        
        except pg.Error as e:
            logger.error(e)
            is_connected = False
            counter_conn_err = counter_conn_err + 1
        except KeyboardInterrupt:
            logger.info('Количество отправленных на запись строк: {}'.format(counter_row))
            logger.info('Количество успешных соединений с базой: {}'.format(counter_conn))
            logger.info('Количество ошибок при работе с базой: {}'.format(counter_conn_err))
            logger.info('Пробуем получить статистику из БД...')
            try:
                    conn = pg.connect(dbname=db_name, user=db_user,password=db_password, host=db_host,port=db_port,connect_timeout=3)
                    cur = conn.cursor()
#                    cur.execute("select count(*) from {};".format(db_table))
#                    logger.info('Количество записанных в бд строк: {}'.format(cur.fetchone()[0]))
                    cur.execute('select count(id), min(ts),max(ts),avg(ts) from (select id, timest-lag(timest) over() as ts from {} order by id desc) as tmp;'.format(db_table))
                    stat_row = cur.fetchone()
                    logger.info('***************************************')
                    logger.info('Строк: {}'.format(stat_row[0]))
                    logger.info('Мин.задержка: {}'.format(stat_row[1]))
                    logger.info('Макс.задержка: {}'.format(stat_row[2]))
                    logger.info('Сред.задержка: {}'.format(stat_row[3]))
                    logger.info('***************************************')
                    if clear == 'yes' :
                        cur.execute('drop table {};'.format(db_table))
                        conn.commit()
                        logger.info('Таблица {} удалена.'.format(db_table))
                    else:
                        logger.info('Не забыть удалить таблицу {} из БД.'.format(db_table))
                    cur.close()
                    conn.close()
                    
            except pg.Error as e:
                logger.error('Ошибка при получении данных из БД.')
                logger.error(e)
                logger.info('Не забыть удалить таблицу {} из БД.'.format(db_table))
            
            
            return



if __name__ == '__main__':
	
	
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
	
    formatLogger = logging.Formatter('%(asctime)s: %(name)-12s: %(funcName)-17s: %(levelname)-8s: %(message)s')
    	
    filehandler = logging.FileHandler('check_connect-{}.log'.format(datetime.datetime.now().strftime("%Y.%m.%d_%H:%M")))
    filehandler.setLevel(logging.INFO)
    filehandler.setFormatter(formatLogger)
	
    logger.addHandler(filehandler)
    
    # Парсер аргументов коммандной строки
    parser = argparse.ArgumentParser(description='Справка по аргументам!')
    parser.add_argument("--h", type=str, help="host database", default='127.0.0.1')
    parser.add_argument("--p", type=str, help="port database", default='5432')
    parser.add_argument("--d", type=str, help="name database", default='postgres')
    parser.add_argument("--U", type=str, help="user database", default='postgres')
    parser.add_argument("--P", type=str, help="passwor user database")
    parser.add_argument("--C", choices=["yes", "no"], type=str, help="Drop table (yes\\no)", default="yes")
        
    parser.add_argument("--console", choices=["yes", "no"],
        default="yes", type=str, help="output log in console, default \"yes\"")
    
    args = parser.parse_args()
    
    if args.console == 'yes':
        formatConsole = logging.Formatter('%(asctime)s: %(levelname)-6s: %(message)s')	
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(formatConsole)
        logger.addHandler(console)
    
	
    logger.info('Начало работы ------------------------------------- ')	
    startTime = datetime.datetime.now()
       
    
    main(db_name=args.d,db_user=args.U,db_password=args.P,db_host=args.h,db_port=args.p,clear=args.C)
        
    stopTime = datetime.datetime.now()    
    logger.info('Окончание работы ------------------------------------- ' )
    logger.info('Времы выполнения скрипта: ' + str(stopTime - startTime))
