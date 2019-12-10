import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext,DataFrame
from pyspark.sql.functions import *
from csv import reader
import json
import time
import re
import os
import math

class files():
	def __init__(self, fname):
		self.files = []
		with open(fname, 'r') as f:
			data = f.read()
			data_list = data[1:-2].replace("'", "").split(', ')
			for file in data_list:
				tmp = file.split('.')
				self.files.append((tmp[0],tmp[1]))

	def getAll(self):
		return self.files

def type_selector(colname):
	s = colname.lower()
	if 'name' in s:
		if 'first' in s or 'last' in s or 'family' in s or 'middle' in s:
			return is_person_name
		elif 'street' in s:
			return is_street_name
		elif 'business' in s:
			return is_business_name
		elif 'school' in s:
			return is_school_name
		elif 'park' in s:
			return is_park_name
	elif 'school' in s and 'level' in s:
		return is_school_level
	elif 'phone' in s or 'phone number' in s or 'tel' in s:
		return is_us_phone_number
	elif 'website' in s or 'web' in s or 'url' in s:
		return is_website
	elif 'latitude' in s:
		return is_lat_coordinates
	elif 'longitude' in s:
		return is_lon_coordinates
	elif 'zip' in s or 'zipcode' in s or 'zip code' in s or 'zip_code' in s:
		return is_zipcode
	elif 'borough' in s:
		return is_borough
	elif 'city' in s:
		return is_city
	elif 'neighborhood' in s:
		return is_neighborhood
	elif 'address' in s:
		return is_address
	elif 'building' in s and 'classification' in s:
		return is_building_classification

def is_person_name(s):
	if re.match('^[a-zA-Z]+,?\s*?[a-zA-Z]*$', s):	# match: alphabet words, then ' ' or ',', alphabet words
		return True
	else:
		return False
def is_street_name(s):
	if not s: return False
	keywords = ['street','avenue','boulevard','road','roadway','court','highway','lane','pavement','place'
				'route','thoroughfare','track','trail','artery','byway','drag','drive','parkway','passage'
				'row','stroll','terrace','turf','trace','turnpike','way']
	for word in keywords:
		if word in s.lower():
			return True
	return False
# hard to find a regular rule, simple check if length >= 2
def is_business_name(s):
	if not s: return False
	if len(s) >= 2:
		return True
	else:
		return False
def is_park_name(s):
	if not s: return False
	if len(s) >=4:
		return True
	else:
		return False
def is_school_name(s):	#include university name
	if not s:return False
	keywords = ['university','school','institute','academy','college']
	for word in keywords:
		if word in s.lower():
			return True
	return False
def is_school_level(s):
	if not s:return False
	keywords = ['elementary','middle','high','yabc','k']
	for word in keywords:
		if word in s.lower():
			return True
	return False
def is_us_phone_number(s):
	if not s: return False
	if re.match('\(?\d{3}[-.)] ?\d{3}[-.]\d{4}', s):
		return True
	else:
		return False
def is_website(s):
	if not s: return False
	if re.match('^(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?$', s):
		return True
	else:
		return False
def is_lat_coordinates(s):
	if not s: return False
	try:
		val = float(s)
	except:
		return False
	else:
		if -90 <= val <= 90:
			return True
		else:
			return False
def is_lon_coordinates(s):
	if not s: return False
	try:
		val = float(s)
	except:
		return False
	else:
		if -180 <= val <= 180:
			return True
		else:
			return False
# five 5 digits and allows the option of having a hyphen and four extended digits
def is_zipcode(s):
	if not s: return False
	if re.match('^\d{5}(?:-\d{4})?', s.strip()):
		return True
	else:
		return False
def is_borough(s):
	if not s: return False
	borough = ['bronx','brooklyn','manhattan','queens', 'staten island', '1', '2','3','4','5', 'mn','bx','bk','qn','si', 'M','X','R','K','Q']
	if s.lower() in borough:
		return True
	else:
		return False
# re match address rule: start with digit or not, match everything following, match 2 letter states and zipcode at end
def is_address(s):
	if not s: return False
	if re.match('(\d+-?\d*)?.*([a-zA-Z]{2})?\d{5}(-\d+)?', s):
		return True
	else:
		return False
# matching rules: alphabetic string including hyphen -, white space
def is_neighborhood(s):
	if not s: return False
	if re.match('[a-zA-Z]+[ -]?[a-zA-Z]*', s):
		return True
	else:
		return False
def is_city(s):
	if not s: return False
	if s.isalpha():
		return True
	else:
		return False
# start with simple: check if size of str >=2
def is_building_classification(s):
	if not s: return False
	if len(s) >=2:
		return True
	else:
		return False

# for vehicle type, make, and color. There is very optimisitc standard representation of data.
# For the purpose of increasing accuracy, we simple apply default method, which check if data exists and apply colname as semantic type.


def semantic_decision(data):
	colname = data[0]
	# if have data in the entry
	if data[1]:
		F = type_selector(colname)
		# if we could find a type function to check with, return default labeling: use colname as semantic type
		if not F:
			#hardcode for colname == 'school' in dataset xne4-4v8f
			if colname.lower() == 'school' and is_school_name(data[1]):
				return (colname, 'school_name')
			return (colname, colname)
		else:
			if F(data[1]):				# if type checking function F return True, meaning it matches labeling condition.
				label = F.__name__[3:]	# extract naming schema from method name
				return (colname, label)
			else:						# type checking function F return False, labeling as other.
				return (colname, 'Other')
	else:
		return (colname, 'NULL')		# Empty data, labeling NULL

def semantic_generator(spark, path, colname):
	df = spark.read.option("delimiter", "\\t").option("header","true").csv(path)
	select_col = df.select(colname).rdd.map(lambda x: (colname,x[0]))
	items_with_semantic = select_col.map(lambda x: semantic_decision(x))
	results = items_with_semantic.map(lambda x:((x[0],x[1]),1)).reduceByKey(lambda x,y:x+y).collect()

	# write to json
	columns = []
	all_semantics = [{'semantic_type': item[0][1], 'count':item[1]} for item in results]
	columns.append({'column_name':colname, 'semantic_types':all_semantics})
	file_name = path.split('/')[-1]
	metadata = {
		'dataset_name': file_name,
		'columns':columns
	}
	return metadata

if __name__ == '__main__':
	# 274 files
	F = files('cluster3.txt')
	files = F.getAll()
	spark = SparkSession.builder.appName("task2_semantic").config("spark.some.config.option", "some-value").getOrCreate()
# 	print('generating semantic for uwyv-629c.tsv.gz')
# 	start = time.time()
# 	metadata = semantic_generator(spark, '/user/hm74/NYCOpenData/{}.tsv.gz'.format(files[0][0]), files[0][1])
	# with open('{}_{}.json'.format(files[0][0],files[0][1]), 'w') as f:
	# 	json.dump(metadata, f, indent=2)
	# end = time.time()
	# print("Time elapsed: " + str(end - start) + " seconds")
	
	
	# count,total = 0, len(files)
	# files_existed = os.listdir('results')
	# error_files = []
	# for file in files:
	# 	# start generate meta from un-generated files.
	# 	if ('{}_{}.json'.format(file[0], file[1])) in files_existed:
	# 		# print('{}_{}.json has already existed.'.format(file[0], file[1]))
	# 		count +=1
	# 		continue
	# 	try:
	# 		# print('generating meta for {}_{}'.format(file[0], file[1]))
	# 		# start = time.time()
	# 		# metadata = semantic_generator(spark, '/user/hm74/NYCOpenData/{}.tsv.gz'.format(file[0]), file[1].replace('_', ' '))
	# 		metadata = semantic_generator(spark, '/user/hm74/NYCOpenData/{}.tsv.gz'.format(file[0]), file[1])
	# 	except:
	# 		print('Exception occurred on ' + file[0])
	# 		print('------------------')
	# 		error_files.append(file)
	# 	else:
	# 		# with open('results/{}_{}.json'.format(file[0], file[1]), 'w') as f:
	# 		# 	json.dump(metadata, f, indent=2)
	# 		# end = time.time()
	# 		# count +=1
	# 		# print("Time elapsed: " + str(end - start) + " seconds")
	# 		# print('{} / {} files finished...'.format(count, total))





