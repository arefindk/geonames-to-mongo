
# coding: utf-8

# In[ ]:


from pymongo import MongoClient
from dateutil import parser
from elasticsearch import Elasticsearch


# ## Setting up mongodb

# In[ ]:


client = MongoClient('mongodb://localhost:27017')
db = client.geonames
collection = db.all_names


# We will use the dataset and fieldnames from this website:
# http://download.geonames.org/export/dump/

# query_lower is the lower case text in which field you will query
# 
# 
# After you get the match, ascii_lower is the name you want to use to disambiguate between cities

# ## Setting up Elasticsearch

# In[ ]:


es = Elasticsearch()


# In[ ]:


line_num = 0
field_names = ["geo_name_id","name", "ascii_name", "alternate_name", "latitude", "longitude", "feature_class",
"feature_code","country_code","cc2","admin1_code","admin2_code","admin3_code","admin4_code","population",
"elevation","dem","timezone","modification_date"]
with open('allCountries.txt','r') as f:
    document = f.xreadlines()
    for line in document:
        line_num += 1
        if line_num % 10000 == 0:
            print line_num
        splitted_line = line.strip().split('\t')
        if splitted_line[3] == "":
            splitted_line[3] = splitted_line[2]
        all_possible_names = splitted_line[3].split(',')
        
        ## Now making sure the name and ascii_names are not ignored in the query 
        if splitted_line[1] not in all_possible_names:
            all_possible_names.append(splitted_line[1])
        if splitted_line[2] not in all_possible_names:
            all_possible_names.append(splitted_line[2])
            
        for alternate_name in all_possible_names:
            #print "## create doc for", alternate_name
            doc = dict(zip(field_names,splitted_line))
            ## We can keep a lower case for ascii_name
            doc['ascii_lower'] = doc['ascii_name'].lower()
            ## we have multiple names in the altrnate_name field
            ## but we need it to be the current alternate name
            doc['alternate_name'] = alternate_name
            ## we will also keep the lower case of the alternate name
            doc['query_lower'] = alternate_name.lower()
            ## geo_name_id should be numeric
            doc['geo_name_id'] = int(doc['geo_name_id'])
            ## both latitude and longitude shoud be float
            doc['latitude'] = float(doc['latitude'])
            doc['longitude'] = float(doc['longitude'])
            ## to comply with mongodb geosearch we will create another location field
            ## it should be in this format, loc : [ <longitude> , <latitude> ]
            doc['loc'] = [doc['longitude'], doc['latitude']]
            doc['population'] = int(doc['population'])
            if doc['elevation'] != '':
                doc['elevation'] = int(doc['elevation'])
            doc['dem'] = int(doc['dem'])
            doc['modification_date'] = parser.parse(doc['modification_date'])
            #print doc
            es.index(index='geonames', doc_type = 'location', id = doc['geo_name_id'], body = doc)
            #collection.insert_one(doc)


# In[ ]:


# We will read the all countries list and create a dictionary where the two digit ISO code
# would have a value that has the three digit ISO-3 code,full name, lower case full name, continent, neighbours 
# as another dictionary

# The length of one line is at least 17 fields.
# For some of the countries there is no Equivalent FIPS code or neighbours, in that case the 
# length of the country is 17. In other cases we might have a list of neighboring countries
# so the length would be 18.
# Only for cook islands (CK) we have an equivalent fips code (I don't know what does that mean) and the length of 
# the field would be 19 in that case.

#ISO	ISO3	ISO-Numeric	fips	Country	Capital	Area(in sq km)	Population	Continent	tld	CurrencyCode	CurrencyName	Phone	Postal Code Format	Postal Code Regex	Languages	geonameid	neighbours	EquivalentFipsCode

country_code_dict = dict()
with open("countryInfo.txt", 'r') as f:
    countries = f.xreadlines()
    for country in countries:
        country =  country.strip()
        if not country.startswith("#"):
            splitted_country = country.rstrip().split("\t")
            doc = dict()
            #print len(splitted_country)
            doc["country_ISO3"] = splitted_country[1]
            doc["country_iso_numeric"] = splitted_country[2]
            doc["country_fips"] = splitted_country[3]
            doc["country_full_name"] = splitted_country[4]
            ## Only cook island in this document has a length of 19 anbd it does not have any neighbor
            ## here. Other 17 length countries do not have neighbors
            if len(splitted_country) == 18:
                doc["country_neighbors"] = splitted_country[17].split(",")
                doc["country_has_neighbors"] = True
            else:
                doc["country_neighbors"] = None
                doc["country_has_neighbors"] = False
            doc["country_capital"] = splitted_country[5]
            doc['country_continent'] = splitted_country[8]
            country_code_dict[splitted_country[0]] = doc


# In[ ]:


country_code_dict['ZM']

