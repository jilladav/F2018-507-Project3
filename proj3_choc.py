import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def create_bars():
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Could not connect to database.")
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)
    statement = '''CREATE TABLE 'Bars' 
    ('Id' INTEGER PRIMARY KEY AUTOINCREMENT, 'Company' Text, 'SpecificBeanBarName' TEXT, 'REF' TEXT,
        'ReviewDate' TEXT, 'CocoaPercent' REAL, 'CompanyLocationId' INT, 'Rating' REAL, 'BeanType' TEXT,
        'BroadBeanOriginId' INT);'''
    cur.execute(statement)

    conn.commit()
    conn.close()

def populate_bars():
    try:
        conn = sqlite3.connect(DBNAME)
        #From https://stackoverflow.com/questions/3425320/sqlite3-programmingerror-you-must-not-use-8-bit-bytestrings-unless-you-use-a-te
        conn.text_factory = str
        cur = conn.cursor()
    except:
        print("Could not connect to database.")

    with open(BARSCSV) as f:
        csvReader = csv.reader(f)
        statement = '''INSERT INTO 'Bars' (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent,
            CompanyLocationId, Rating, BeanType, BroadBeanOriginId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        for row in csvReader:
            if row[0] == "Company":
                continue
            cocoa = row[4][:-1]
            cur.execute(statement, (row[0], row[1], row[2], row[3], cocoa, row[5], row[6], row[7], row[8]))

    conn.commit()
    conn.close()

def create_countries():
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Could not connect to database.")

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)
    statement = '''CREATE TABLE 'Countries' 
    ('Id' INTEGER PRIMARY KEY AUTOINCREMENT, 'Alpha2' Text, 'Alpha3' TEXT, 'EnglishName' TEXT,
        'Region' TEXT, 'Subregion' TEXT, 'Population' INT, 'Area' REAL);'''
    cur.execute(statement)

    conn.commit()
    conn.close()  

def populate_countries():
    try:
        conn = sqlite3.connect(DBNAME)
        #From https://stackoverflow.com/questions/3425320/sqlite3-programmingerror-you-must-not-use-8-bit-bytestrings-unless-you-use-a-te
        conn.text_factory = str
        cur = conn.cursor()
    except:
        print("Could not connect to database.")

    f = json.load(open('countries.json'))
    statement = '''INSERT INTO 'Countries' (Alpha2, Alpha3, EnglishName, Region, Subregion,
            Population, Area) VALUES (?, ?, ?, ?, ?, ?, ?)'''
    
    for country in f:
        cur.execute(statement, (country['alpha2Code'], country['alpha3Code'], country['name'], country['region'], country['subregion'], country['population'], country['area']))

    conn.commit()
    conn.close()

def update_tables_with_foreign_keys():
    try:
        conn = sqlite3.connect(DBNAME)
        #From https://stackoverflow.com/questions/3425320/sqlite3-programmingerror-you-must-not-use-8-bit-bytestrings-unless-you-use-a-te
        conn.text_factory = str
        cur = conn.cursor()
    except:
        print("Could not connect to database.")


    f = json.load(open(COUNTRIESJSON))

    for country in f:
        country_id = cur.execute("SELECT id FROM 'Countries' WHERE EnglishName=?", (country['name'],)).fetchone()[0]
        statement = '''UPDATE Bars SET CompanyLocationId = ? WHERE CompanyLocationId LIKE ? '''
        cur.execute(statement,(country_id,'%'+country['name']+'%'))
        statement = '''UPDATE Bars SET BroadBeanOriginId = ? WHERE BroadBeanOriginId LIKE ? '''
        cur.execute(statement,(country_id,'%'+country['name']+'%'))


    conn.commit()
    conn.close()

create_bars()
populate_bars()

create_countries()
populate_countries()

update_tables_with_foreign_keys()
# Part 2: Implement logic to process user commands
def process_command(command):
    try:
        conn = sqlite3.connect(DBNAME)
        #From https://stackoverflow.com/questions/3425320/sqlite3-programmingerror-you-must-not-use-8-bit-bytestrings-unless-you-use-a-te
        conn.text_factory = str
        cur = conn.cursor()
    except:
        print("Could not connect to database.")
    command = command.split()
    if command[0] == "bars":
        sortby = 'rating'
        number = 10
        sortby_query = " ORDER BY rating DESC LIMIT ?"
        country_query = "None"
        country = ""
        for word in command:
            if "sellcountry" in word:
                country = word[-2:]
                country_query = " WHERE c.Alpha2= ?"
            elif "sourcecountry" in word:
                country = word[-2:]
                country_query = " WHERE z.Alpha2 = ?"
            elif "sellregion" in word:
                split_word = word.split('=')
                country = split_word[1]
                country_query = " WHERE c.Region = ? "
            elif "sourceregion" in word:
                split_word = word.split('=')
                country = split_word[1]
                country_query = " WHERE z.Region = ? "
            elif word == "cocoa":
                sortby = "CocoaPercent"
            elif "bottom" in word:
                split_word = word.split('=')
                number = int(split_word[1])
                sortby_query = " ORDER BY " + sortby + " LIMIT ?"
            elif "top" in word:
                split_word = word.split('=')
                number = int(split_word[1])      
        base_statement = '''SELECT Bars.SpecificBeanBarName, Bars.Company, c.EnglishName, Bars.Rating, Bars.CocoaPercent, z.EnglishName
        FROM Bars
        JOIN Countries as c ON Bars.CompanyLocationId = c.id
        JOIN Countries as z ON Bars.BroadBeanOriginId = z.id'''
        if country_query != "None":
            statement = base_statement + country_query + sortby_query
            cur.execute(statement,(country,number))
        else:
            statement = base_statement + sortby_query
            cur.execute(statement,(number,))

        lst = cur.fetchall()


        
    conn.close()

    return lst


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue
    return

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
