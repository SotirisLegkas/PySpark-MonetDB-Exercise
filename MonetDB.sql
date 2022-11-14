create or replace LOADER myloader(filename string) LANGUAGE PYTHON {
import pandas as pd
willow = pd.read_csv(filename,sep=',').to_dict('list')
_emit.emit(willow)};


CREATE TABLE willow FROM LOADER myloader('C:/....../zillow.csv' );

select * from willow limit 10;

--Extract number of bedrooms. You will implement a UDF that processes the `facts_and_features` column and extracts the number of bedrooms.

create or replace FUNCTION get_bedrooms(i STRING)
RETURNS INTEGER
LANGUAGE PYTHON {
x=[]
for j in range(len(i)):
    if i[j].split()[0]=="None":
        x.append(0)
    else:
        x.append(int(i[j].split()[0]))
return x};

select *,get_bedrooms("facts and features") as bedrooms from willow limit 10;


--Extract number of bathrooms. You will implement a UDF that processes the `facts_and_features` column and extracts the number of bathrooms.

create or replace FUNCTION get_bathrooms(i STRING)
RETURNS INTEGER
LANGUAGE PYTHON {
x=[]
for j in range(len(i)):
    if i[j].split()[2]=="None":
        x.append(0)
    else:
        x.append(int(float(i[j].split()[2])))
return x};

select *,get_bathrooms("facts and features") as baths from willow limit 10;

--Extract sqft. You will implement a UDF that processes the `facts_and_features` column and extracts the sqft.

create or replace FUNCTION get_sqft(i STRING)
RETURNS INTEGER
LANGUAGE PYTHON {
x=[]
for j in range(len(i)):
	if i[j].split(',')[2].split()[0]=="None":
		x.append(0)
	else:
		x.append(int(i[j].split(',')[2].split()[0]))
return x};

select *,get_sqft("facts and features") as sqft from willow limit 10;

--Extract type. You will implement a UDF that processes the `title` column and returns the type of the listing (e.g. condo, house, apartment)

CREATE OR REPLACE FUNCTION get_type(i STRING)
RETURNS string 
LANGUAGE PYTHON {
x=[]
for j in range(len(i)):
	if 'condo' in i[j].lower():
		x.append('Condo')
	elif 'house' in i[j].lower():
		x.append('House')
	elif 'land' in  i[j].lower():
		x.append('Lot/Land')
	elif 'construction' in i[j].lower():
		x.append('New Construction')
	elif 'home' in i[j].lower():
		x.append('Family Home')
	else:
		x.append('Other Type')
return x};

select *,get_type("title") as type from willow limit 10;

--Extract offer. You will implement a UDF that processes the `title` column and returns the type of offer. This can be `sale`, `rent`, `sold`, `forclose`.

CREATE OR REPLACE FUNCTION get_offer(i STRING)
RETURNS string 
LANGUAGE PYTHON {
x=[]
for j in range(len(i)):
	if 'sale' in i[j].lower():
		x.append('sale')
	elif 'rent' in i[j].lower():
		x.append('rent')
	elif 'sold' in  i[j].lower():
		x.append('sold')
	elif 'foreclosure' in i[j].lower():
		x.append('foreclose')
	else:
		x.append('Other State')
return x};

select *,get_offer("title") as offer from willow limit 10;


--Create table with UDFs

create table final_willow as select *, get_bedrooms("facts and features") as bedrooms, get_bathrooms("facts and features") as bathrooms, get_sqft("facts and features") as sqft, get_type("title") as type, get_offer("title") as offer from willow;

--Filter out listings that are not for sale

create table filtered_sales as select * from final_willow where (offer= 'sale');

--Extract price. You will implement a UDF that processes the `price` column and extract the price. Prices are stored as strings in the CSV. This UDF parses the string and returns the price as an integer.

CREATE OR REPLACE FUNCTION get_price(i STRING) 
RETURNS integer 
LANGUAGE PYTHON {
x=[]
for j in range(len(i)):
	x.append(int(float(i[j][1:].replace(',','').replace('+',''))))
return x};

select *,get_price("price") as prices from willow limit 10;

--Filter out listings with more than 10 bedrooms
--Filter out listings with price greater than 20000000 and lower than 100000
--Filter out listings that are not houses

create table filtered as select * from (select *, get_price("price") as prices from filtered_sales) as price_table where (bedrooms <=10) and (prices > 100000) and (prices < 20000000) and (type= 'House');

select * from filtered limit 10;

--Calculate average price per sqft for houses for sale grouping them by the number of bedrooms.

select avg(prices/sqft) as avg_price_per_sqft  ,bedrooms from filtered group by bedrooms;
