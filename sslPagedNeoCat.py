import urllib2
from BeautifulSoup import BeautifulSoup
import re
import time
import pdb
from ssl import SSLError
from py2neo import authenticate, Graph, Node, Relationship
#from collections import namedtuple

path = "https://en.wikipedia.org"
authenticate("localhost:7474", "neo4j", "gb96bhargav")
graph = Graph()

def crawl(url, cat, dad_node):

#	pdb.set_trace()
	retry = 0
	while True:
		time.sleep(6)

		try:
			print "---requested---"
			response = urllib2.urlopen("https://en.wikipedia.org" + str(url) + str(cat), timeout = 600)	
			print "---response acked---"

			soup = BeautifulSoup(response)

			pages = []
			urlp = []

			divp = soup.findAll('div', attrs={'id' : 'mw-pages'})
			patp = re.compile('/wiki/')
			for divs in divp:
				div2 = divs.findAll('div', attrs={'class' : 'mw-content-ltr'})
				for divss in div2:
					linksp = divss.findAll('a', href = True)
					for linkp in linksp:
						stp = str(linkp['href'])
						if patp.match(stp):
							pages.append(linkp.contents[0])
							urlp.append(path + stp)
		#					print linkp.contents[0]

		#	for eachpage in pages:
		#		print eachpage

			pageNode = Node("pages", name = pages, url = urlp)
			graph.create(pageNode)
			rel = Relationship(dad_node, "CONTAINED IN", pageNode)
			graph.create(rel)

			
			division = soup.findAll('div', attrs={'id' : 'mw-subcategories'})
			pat = re.compile('/wiki/Category:')
			pattern = "/wiki/Category:"
			for div in division:
				links = div.findAll('a', href = True)
				for link in links:
					st = str(link['href'])
					if pat.match(st):
						#print st.split(pattern,1)[1] #.contents[0]
		#				graph.cypher.execute("MATCH(r:category {name:'" + cat + "'})")
		#				graph.cypher.execute("CREATE (c:category {name: '" + str(link.contents[0]) + "'})")
		#				graph.cypher.execute("CREATE (r)-[:CLASSIFIES]->(c)")

		#				pdb.set_trace()
						subcatNode = Node("category", name = str(link.contents[0]), url = path + st)
						graph.create(subcatNode)
						cat_classifies_subcat = Relationship(dad_node, "CLASSIFIES_INTO", subcatNode)
						graph.create(cat_classifies_subcat)
						print link.contents[0]
						crawl("", str(st), subcatNode)


		except (Exception, SSLError) as e:
			#pdb.set_trace()
			if retry > 25:
				pdb.set_trace()
			retry += 1
			print "OOPS!!! " + str(e)
		else:
			break

#crawl("/wiki/Category:Comics_industry")

#graph.cypher.execute("CREATE (r:root {name: 'Universe'})") 

univ = Node("root", name = "Universe", url = "https://en.wikipedia.org/wiki/Portal:Contents/Categories")
graph.create(univ)

mainTopics = ['Culture', 'Research', 'Library_science', 'Arts', 'Geography', 'Places', 'Health', 'Self_care', 'Healthcare_occupations', 'History', 'Events', 'Formal_sciences', 'Science', 'Natural_science', 'Nature', 'People', 'Personal_life', 'Self', 'Surnames', 'Philosophy', 'Thought', 'Religion', 'Belief', 'Society', 'Social_sciences', 'Technology', 'Applied_sciences'];


for topic in mainTopics:
#	graph.cypher.execute("CREATE (c:category {name: '"+topic+"'})")
	catNode = Node("category", name = topic, url = path + "/wiki/Category:" + topic)
	graph.create(catNode)
#	graph.cypher.execute("CREATE (r)-[:CLASSIFIES]->(c)")
	dad_knows_child = Relationship(univ, "CLASSIFIES_INTO", catNode)
	graph.create(dad_knows_child)
	crawl("/wiki/Category:", topic, catNode)

