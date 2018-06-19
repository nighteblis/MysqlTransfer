#!/bin/python3
# -*- coding: utf-8 -*-

import pymysql
import yaml
import datetime


if False: '''
根据规则来拉取数据库数据并导出到sql文件
nighteblis@gmail.com
'''


class FetchDtabaseData:
	dataServer=""
	user=""	
	password=""
	database=""
	connection=None
	rules=None

	def __init__(self,dataServer,user,password,database,rules):
		self.dataServer=dataServer
		self.user=user
		self.password=password
		self.database=database
		self.rules=rules
		self.connection=pymysql.connect(self.dataServer,self.user,self.password,self.database ,charset='utf8')

	def __executeSql(self,sql):
		cursor=self.connection.cursor()
		#if sql != "show tables":
		cursor.execute(sql)
		return cursor

	def getAllTables(self):
		tables = []
		cursor=self.__executeSql("show tables")
		#cursor.fetchall()
		
		for (table_name,) in cursor:
			tables.append(table_name)
			#print(table_name)
		cursor.close()
		print(tables)
		return tables	


#	def getDataByPage(self,)


	def generateSql(self,table,sqldata,columns):
		for row in sqldata:
			print(row)
			newrow=self.transfersqlData(row)
			generatedsql="insert into "+table+" "+str(columns)+" values " +str(newrow) + ";";
			print(generatedsql)
				#print(column)
			break
		#return encoded

	def transfersqlData(self,sqldata):
		newdata=[]
		for column in sqldata:
			#print(type(column))
			if type(column) is datetime.datetime:
				print("=============="+str(column))
				column=str(column)
			newdata.append(column)
		return tuple(newdata)
	

	def getColumnsFromCursorDescription(self,description):
		columns = []
		for column in description:
			columns.append(column[0])	
		print(tuple(columns))
		return tuple(columns)
	
	def getDataFromTableBasedRules(self,table):
		print(table)
		tableRule=None
		tableColumns=[]
		continueSelect=True
		start=1
		end=200

		if table in self.rules:
 			tableRule=rules['']

		if tableRule is not None:
			while continueSelect:
				cursor=self.__executeSql('select * from '+table + ' '+tableRule + ' limit '+str(start) +','+str(end))
				if len(tableColumns) == 0:
					tableColumns=self.getColumnsFromCursorDescription(cursor.description)

				#print(tableColumns)

				results = cursor.fetchall()
				self.generateSql(table,results,tableColumns)
				cursor.close()
				continueSelect = False

		else:
			while continueSelect:
				cursor=self.__executeSql('select * from '+table + ' limit '+str(start) +','+str(end))
				if len(tableColumns) == 0:
					tableColumns=self.getColumnsFromCursorDescription(cursor.description)

				#print(tableColumns)

				results = cursor.fetchall()
				self.generateSql(table,results,tableColumns)

				cursor.close()
				continueSelect = False



class YamlParser:
	yamlLocation=""
	
	def __init__(self,yamlLocation):
		self.yamlLocation=yamlLocation
	
	def parser(self):
		with open(self.yamlLocation, 'r') as stream:
			try:
				rules=yaml.load(stream)
				#print(rules)
				return rules

			except yaml.YAMLError as exc:
				print(exc)
		
		
	

def main():
	parser=YamlParser("rules2.yaml")
	rules=parser.parser()
	print(rules)
	print(rules['database'])
	for db in rules['database']:
		print("==============="+db+ "  tables:=======================")
		print(rules['database'][db])
		databaseRules=rules['database'][db]
		db=FetchDtabaseData(databaseRules['serverAddress'],databaseRules['username'],databaseRules['password'],db,rules)
		tables=db.getAllTables()
		for table in tables:
			db.getDataFromTableBasedRules(table)

if __name__ == "__main__": main()
