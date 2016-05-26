
from django import template
from django.template.loader import get_template 
from django.shortcuts import render
from django.http import HttpResponse
from cassandra.cluster import Cluster
from geopy.geocoders import Nominatim

# Get information from database
def getInfoFromDB(stringval):
	geolocator = Nominatim()
	try:
		location = geolocator.geocode(str(stringval))
	except (ValueError, GQueryError):
		return
	longitude = int(location.longitude)
	latitude = int(location.latitude)

	#Retrive the top 3 most popular hashtag from database
	node = ['127.0.0.1']
	cluster = Cluster(node)
	session = cluster.connect()
	session.set_keyspace("tweets")
	out = session.execute('select max(occurence) from tweets.popularhashtags where longitude=' + str(longitude) + ' and latitude=' + str(latitude) + ' ')[0]
	mostPopularHashtag = session.execute('select hashtag from tweets.popularhashtags where longitude=' + str(longitude) + ' and latitude=' + str(latitude) + ' and occurence=' + str(out.system_max_occurence) + ' ')[0]
	out2 = session.execute('select max(occurence) from tweets.popularhashtags where longitude=' + str(longitude) + ' and latitude=' + str(latitude) + ' and occurence<' + str(out.system_max_occurence) + " ")[0]
	popularhashtags2 = session.execute('select hashtag from tweets.popularhashtags where longitude=' + str(longitude) + ' and latitude=' + str(latitude) + ' and occurence=' + str(out2.system_max_occurence) + ' ')[0]
	out3 = session.execute('select max(occurence) from tweets.popularhashtags where longitude=' + str(longitude) + ' and latitude=' + str(latitude) + ' and occurence<' + str(out2.system_max_occurence) + " ")[0]
	popularhashtags3 = session.execute('select hashtag from tweets.popularhashtags where longitude=' + str(longitude) + ' and latitude=' + str(latitude) + ' and occurence=' + str(out3.system_max_occurence) + ' ')[0]
	
	returnVal = {
	'mostPopularHash': mostPopularHashtag.hashtag,
	'mostPopularHash2': popularhashtags2.hashtag,
	'mostPopularHash3': popularhashtags3.hashtag,
	'origString': stringval,
	}

	return returnVal

# index handles server sided requests
def index(request):

	mostPopularHashtag = ''
	origString = 'new york'

	returnVal = {
	'mostPopularHash': '',
	'mostPopularHash2': '',
	'mostPopularHash3': '',
	'origString': 'new york',
	}

	if(request.GET.get('search')):
		origString = request.GET['search']
		returnVal = getInfoFromDB(request.GET['search'])

	return render(request, 'twitterSearch/index.html', returnVal)
