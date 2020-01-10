import json
import os
import pandas as pd


output_files = os.listdir('results')

res = []
for file in output_files:
	colname = file.split('.')[0]
	with open('results/{}'.format(file)) as f:
		data = json.load(f)
		# update the column name with proper format
		data['columns'][0]['column_name'] = colname
		res.append(data['columns'][0])

output = {'predicted_types': res}
with open('task2.json','w') as f:
	json.dump(output, f, indent=2)


df = pd.ExcelFile('task2_true_label.xlsx').parse('Sheet 1 - task2_label copy')
new_df = df[['Dataset','Label']]

res2 = []
for i in range(len(new_df)):
	dataset = new_df.loc[i]['Dataset'].strip('\'')
	f1,f2 = dataset.split('.', maxsplit=1)
	f2 = f2.split('.',maxsplit=1)[0]
	colname = f1 + '_' + f2
	label = new_df.loc[i]['Label']
	res2.append({'column_name':colname, 'manual_labels':[{'semantic_type':label}]})

output = {'actual_types': res2}
with open('task2-manual-labels.json','w') as f:
	json.dump(output, f, indent=2)



