# reference: https://learn.microsoft.com/zh-tw/sql/linux/sql-server-linux-migrate-bcp?view=sql-server-linux-2017
import sys
import os
import random
import time
host = "10.28.64.4"
user = "sqlserver"
password = "password"
database = "BcpSampleDB"
table = "TestEmployees"
file_name = "test_data.txt"
name_list = ["Jared", "Nikita", "Tom", "Liam", "Noah", "Oliver", "James", "Elijah", "William", "Henry", "Lucus", "Benjamin", "Theodore", "Duttom", "Kayce", "Chosen", "Khaza", "Waylen", "Asaiah"]
country_list = ["Australia", "India", "Germany", "Canada", "China", "America", "Combodia", "Czench Republic", "Bangladesh", "Afghanistan", "Argentina", "Albania", "Azerbajan", "Belgium", "Bulgaria"]
numbers = 10000000 # Row
targetsize = 100000 # MB
NumOfTimes = 10000
DataRange = 1000000000


def create_database():
    cmd = 'sqlcmd -S %s -U %s -P %s -Q "CREATE DATABASE %s;"' % (host, user, password, database)
    print(cmd)
    output = os.popen(cmd).read()
    print(output)


def create_table():
    cmd = 'sqlcmd -S %s -U %s -P %s -d %s -Q "CREATE TABLE %s (Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY, Name NVARCHAR(50), Location NVARCHAR(50));"' % (host, user, password, database, table)
    print(cmd)
    output = os.popen(cmd).read()
    print(output)


def create_data(start):
	with open(file_name, "w") as f:
		for i in range(numbers):
			name = "%s-%s-%s" % (random.choice(name_list), int(random.randint(0, DataRange)), int(random.randint(0, DataRange)))
			country = "%s-%s-%s" % (random.choice(country_list), int(random.randint(0, DataRange)), int(random.randint(0, DataRange)))
			data = "%s,%s,%s" % (i+start, name, country)
			f.write("%s\n" % data)
		f.close()
	print("create %s data in %s" % (numbers, file_name))


def import_data():
    cmd = "bcp %s in ~/test_data.txt -S %s -U %s -P %s -d %s -c -t  ','" % (table, host, user, password, database)
    output = os.popen(cmd).read()
    print(output)


def show_data():
	# cmd = 'sqlcmd -S %s -d %s -U %s -P %s -I -Q "SELECT * FROM %s;"' % (host, database, user, password, table)
	cmd = 'sqlcmd -S %s -d %s -U %s -P %s -I -Q "SELECT TOP 3 * FROM %s ORDER BY ID DESC;"' % (host, database, user, password, table)
	print(cmd)
	output = os.popen(cmd).read()
	print(output)
	NumOfData = 0
	try:
		NumOfData = int(output.split("\n")[2].split()[0])
	except Exception:
		pass
	print("Total: %s data in DB" % NumOfData)
	return NumOfData


def show_size():
	cmd = 'sqlcmd -S %s -d %s -U %s -P %s -I -Q "USE %s; EXEC sp_spaceused;"' % (host, database, user, password, database)
	output = os.popen(cmd).read()
	print(output)
	datasize = 0
	try:
		datasize = int(float(output.split("\n")[3].split()[1]))
	except Exception:
		pass
	return datasize


def main(argv):
	if argv == "create-data":
		start = show_data() + 1
		create_data(start)
	elif argv == "create-db":
		create_database()
	elif argv == "create-table":
		create_table()
	elif argv == "import-data":
		import_data()
		show_data()
	elif argv == "gen-data":
		start_time = time.time()
		for i in range(NumOfTimes):
			print("--- %sth generate data ---" % i)
			start = show_data() + 1
			create_data(start)
			import_data()
			show_data()
			datasize = show_size()
			end_time = time.time()
			diff_time = end_time - start_time
			print("%sth generate data by %s seconds" % (i, diff_time))
			if datasize >= targetsize:
				print("Data has %sMB >= 1TB" % datasize)
				break
	else:
		show_data()
		show_size()


if __name__ == '__main__':
    argv = sys.argv[1]
    main(argv)
