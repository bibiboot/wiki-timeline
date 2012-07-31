import re
import pdb
import redis
import pprint
import urllib2
import editdist
from country import *

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r1 = redis.StrictRedis(host='localhost', port=6379, db=1)
p = re.compile('1[89]\d{2}')

def fetch(name):
    """
    Fetch data from api
    Set in redis
    """
    url = 'https://en.wikipedia.org/w/api.php?action=parse&format=json&page=%s&prop=wikitext' % name
    data = urllib2.urlopen(url).read()
    data = cleandata(data)
    data = eval(data)
    #set data in redis
    r.set(name,data)
    return str(data)

def geodata():
    """
    Set the countries in redis
    """
    geo = {}
    for key  in country:
        name = key['ISO_3166-1_Country_name'].lower()
        code = key['ISO_3166-1_Alpha-2_Code_element']
        geo[name] = code
    geo['usa'] = "USA"
    geo['uk'] = 'UK'
    geo['england'] = 'UK'
    geo['nyc'] = 'NYC'
    geo['california'] = 'California'
    geo['sa'] = 'South Africa'
    r.set('geo', geo)
    return str(geo)

def run(name):
    """
    Create a timelime with relevant content
    List containing the lines with year in it
    """
    timeline = []
    data = r.get(name)
    if not data:
        #data not present in redis 
        data = fetch(name)
    data = eval(data)
    content = data['parse']['wikitext']['*']
    for d in content.split('==='):
        [ timeline.append(line) for line in d.split('.') if p.findall(line) ]
    return timeline

def tline(name, timeline):
    """
    Fetch year and place from 
    required lines
    """
    result = {name: []}
    verb = None
    geo = r.get('geo')
    if not geo:
        #data not in redis
        geo = geodata()
    geo = eval(geo)

    for line in timeline:
         
        line = cleanline(line) 
        verb = None
        placelist = []
        istravel = False
        yearlist = p.findall(line)
        
        #print line
        #pdb.set_trace()
        for word in line.split(' '):
            word = word.strip()
            if word == '':
                continue
            #clean the word
            word = cleanword(word)
            #check if the word is a country
            if word == 'sa':
                #pdb.set_trace()
                pass
            if geo.has_key(word.strip()):
                placelist.append(word)
            #this line contains the travel verb
            wordstatus = istravelword(word)
            if wordstatus:
                #print word
                verb = word
            istravel|= wordstatus
        if istravel and placelist and yearlist and len(yearlist) < 3:
            result[name].append((verb, placelist, yearlist))
    return result 

def cleanword(word):
    """
    Clean the word
    """
    word = word.lower()
    return word

def cleanline(line):
    """
    Clean the lines 
    """
    line = line.replace('(','')
    line = line.replace(')','')
    line = line.replace(',','')
    line = line.replace(']','')
    line = line.replace('[','')
    line = line.replace('|',' ')
    line = line.replace('United States', 'USA')
    line = line.replace('United Kingdom','UK')
    line = line.replace('New York','NYC')
    line = line.replace('South Africa','SA')
    return line

def cleandata(data):
    data = data.replace('Co.','')
    return data

def years(line):
    """
    List of years in the line
    """
    yearlist = p.findall(line)
    return yearlist

def istravelword(word):
    """
    Match if word is similar to the travel word
    """
    return similar(word)

def similar(word):
    """
    Find similar movies
    """
    verblist = [ 'went',
                 'gone', 
                 'move', 
                 'travelled',
                 'moved', 
                 'made',
                 'born', 
                 'shot', 
                 'die', 
                 'directed',
                 'accepted',
                  'found',
                  'developed',
                  'came',
                  'returned',
                 'fled']
    if word in verblist:
        return True
    return False
    """
    if word in ['the','in','his','her'] or len(word) < 4:
        return False
    max_edit_dist = len(word)/3
    max_edit_dist = 3
    if max_edit_dist < 4:
        max_edit_dist = len(title)/2
    verblist = [ 'went','gone', 'move', 'born', 'shot', 'die', 'direct']
    length = len(verblist)
    for index in xrange(length):
        if editdist.distance(verblist[index], word) < max_edit_dist:
            #print word + " is similar to " + verblist[index]
            return True
    return False
    """
def add(result):
    for person, datalist in result.items():
        for data in datalist:
            for place in data[1]:
                r1.zadd(person+'_t',int(data[2][0]), place)        
                r1.zadd(place, int(data[2][0]), person)
    pprint.pprint(result)

def execute():    
    persons = [
           'Roman Polanski',
           'John Lennon',
           'Mohandas Karamchand Gandhi'
           ]
    for person in persons:
        person= person.replace(' ','%20')
        timeline = run(person)
        result = tline(person, timeline)
        add(result)

execute()
