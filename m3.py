#!/usr/bin/env python3
import csv
import psycopg2
import sys
from datetime import datetime
import time
from pytz import timezone



con = psycopg2.connect(
			dbname='db36',
			user='', 
            password='', 
            host='localhost')
cur = con.cursor()



# now_utc2 = datetime.now(timezone('Europe/Minsk')).date()

try:
	cur.execute("""SELECT "TimeStartJ" FROM core_book ORDER BY "TimeStartJ" DESC LIMIT 1;""")
	result = cur.fetchone()
	now_utc2 = result[0]
	str_utc2 = now_utc2.strftime("%Y-%m-%d %H:%M:%S")
	
except:
	str_utc2 = '1920-02-03 00-00-00'
	



def str2datetime(value):
	try:
		time_bd = str(value)
		if time_bd > str_utc2:
			return datetime.strptime(value, '%Y-%m-%d %H:%M:%S').date()
		else:
			return datetime(1980, 12, 12)
	except:
		return datetime(1980, 12, 12)


def str3datetime(value):
	try:
		return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
	except:
		return datetime(1980, 12, 12)


fname = (sys.argv[1])
index = (sys.argv[2])



with open(fname) as csv_file:
	csv_reader = csv.reader(csv_file, delimiter=',')
	for key,row in enumerate(csv_reader):
		# if key == 0:
		# 	continue
		print(row)
		PustotaA=str(index)
		Tel1B=row[1]
		Tel2C=row[2]
		Typ_zvonkaD=row[3]
		KtoE=row[4]
		SIPF=row[5]
		SIPG=row[6]
		StatusH=row[7]
		SIPI=row[8]
		TimeStartJ=row[9]
		TimeMidK=row[10]
		TimeFFL=row[11]
		Number1M=row[12]
		Number2N=row[13]
		StateO=row[14]
		DocP=row[15]
		IdQ=row[16]
		TimeStartJ2=row[9]
		try:
			cur.execute(f"""INSERT INTO core_book("PustotaA",
				"Tel1B",
				"Tel2C",
				"Typ_zvonkaD",
				"KtoE",
				"SIPF",
				"SIPG",
				"StatusH",
				"SIPI",
				"TimeStartJ",
				"TimeMidK",
				"TimeFFL",
				"Number1M",
				"Number2N",
				"StateO",
				"DocP",
				"IdQ",
				"TimeStartJ2")VALUES ('{PustotaA}',
				'{Tel1B}',
				'{Tel2C}',
				'{Typ_zvonkaD}',
				'{KtoE}',
				'{SIPF}',
				'{SIPG}',
				'{StatusH}',
				'{SIPI}',
				'{str3datetime(TimeStartJ)}',
				'{TimeMidK}',
				'{str3datetime(TimeFFL)}',
				'{Number1M}',
				'{Number2N}',
				'{StateO}',
				'{DocP}',
				'{IdQ}',
				'{str2datetime(TimeStartJ2)}')""")
				
			con.commit()
		except psycopg2.errors.InvalidDatetimeFormat:
			continue



cur.execute("""DELETE FROM core_book WHERE id IN (SELECT id FROM core_book WHERE "TimeStartJ2" = '1980-12-12');""")


con.commit()

cur.close()
con.close()
