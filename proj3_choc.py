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

    try:
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
    except:
        print("Could not create table Bars.")

def populate_bars():
    try:
        conn = sqlite3.connect(DBNAME)
        #From https://stackoverflow.com/questions/3425320/sqlite3-programmingerror-you-must-not-use-8-bit-bytestrings-unless-you-use-a-te
        conn.text_factory = str
        cur = conn.cursor()
    except:
        print("Could not connect to database.")

    try:
        with open(BARSCSV) as f:
            csvReader = csv.reader(f)
            statement = '''INSERT INTO 'Bars' (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent,
                CompanyLocationId, Rating, BeanType, BroadBeanOriginId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            for row in csvReader:
                if row[0] == "Company":
                    continue
                cocoa = ((float(row[4][:-1])) / 100)
                cur.execute(statement, (row[0], row[1], row[2], row[3], cocoa, row[5], row[6], row[7], row[8]))

        conn.commit()
        conn.close()
    except:
        print("Could not populate table Bars.")

def create_countries():
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Could not connect to database.")
    try:
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
    except:
        print("Could not create table Countries.")  

def populate_countries():
    try:
        conn = sqlite3.connect(DBNAME)
        #From https://stackoverflow.com/questions/3425320/sqlite3-programmingerror-you-must-not-use-8-bit-bytestrings-unless-you-use-a-te
        conn.text_factory = str
        cur = conn.cursor()
    except:
        print("Could not connect to database.")

    try:
        f = json.load(open('countries.json'))
        statement = '''INSERT INTO 'Countries' (Alpha2, Alpha3, EnglishName, Region, Subregion,
                Population, Area) VALUES (?, ?, ?, ?, ?, ?, ?)'''
        
        for country in f:
            cur.execute(statement, (country['alpha2Code'], country['alpha3Code'], country['name'], country['region'], country['subregion'], country['population'], country['area']))

        cur.execute(statement, ('UN', 'UNK', 'Unknown', 'Unknown', '', 0, 0)) 
        
        conn.commit()
        conn.close()
    except:
        print("Could not populate table Countries.")

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
        #statement = '''UPDATE Bars SET CompanyLocationId = ? WHERE CompanyLocationId LIKE ? '''
        #cur.execute(statement,(country_id,'%'+country['name']+'%'))
        #statement = '''UPDATE Bars SET BroadBeanOriginId = ? WHERE BroadBeanOriginId LIKE ? '''
        #cur.execute(statement,(country_id,'%'+country['name']+'%'))
        statement = '''UPDATE Bars SET CompanyLocationId = ? WHERE CompanyLocationId = ?'''
        cur.execute(statement,(country_id, country['name']))
        statement = '''UPDATE Bars SET BroadBeanOriginId = ? WHERE BroadBeanOriginId = ? '''
        cur.execute(statement,(country_id, country['name']))
        statement = '''UPDATE Bars SET BroadBeanOriginId = ? WHERE BroadBeanOriginId = ? '''
        cur.execute(statement,(251, "Unknown"))


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
    lst = []
    command_split = command.split()
    if command_split[0] == "bars":
        sortby = 'rating'
        number = 10
        sortby_query = " ORDER BY rating DESC LIMIT ?"
        country_query = "None"
        country = ""
        bottom = False
        for word in command_split:
            if "sellcountry" in word:
                country = word[-2:]
                country_query = " WHERE c.Alpha2= ? AND c.EnglishName != \"Unknown\""
            elif "sourcecountry" in word:
                country = word[-2:]
                country_query = " WHERE z.Alpha2 = ? AND z.EnglishName != \"Unknown\""
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
                bottom = True
            elif "top" in word:
                split_word = word.split('=')
                number = int(split_word[1])  
                sortby_query = " ORDER BY " + sortby + " DESC LIMIT ?"
            elif word == "ratings":
                continue 
            elif word == "bars":
                continue
            else:
                print("Command not recognized: " + command)
                return

        if not bottom:
            sortby_query = " ORDER BY " + sortby + " DESC LIMIT ?"

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

    elif command_split[0] == "companies":
        select_statement = '''SELECT Bars.Company, Countries.EnglishName, AVG(Bars.Rating) '''
        join_statement = '''FROM Bars JOIN Countries ON Countries.Id = Bars.CompanyLocationId'''
        number = 10
        sortby_query = ''' ORDER BY AVG(Bars.Rating) DESC LIMIT ?'''
        sortby = "AVG(Bars.Rating)"
        groupby_query = ''' GROUP BY Bars.Company HAVING COUNT(*) > 4'''
        country = ""
        country_query = "None"
        bottom = False
        for word in command_split:
            if "country" in word:
                country = word[-2:]
                country_query = " WHERE Countries.Alpha2= ? AND Countries.EnglishName != \"Unknown\""
            elif "region" in word:
                split_word = word.split('=')
                country = split_word[1]
                country_query = " WHERE Countries.Region = ? AND Countries.EnglishName != \"Unknown\""
            elif word == "cocoa":
                sortby = "AVG(Bars.CocoaPercent)"
                select_statement = '''SELECT Bars.Company, Countries.EnglishName, AVG(Bars.CocoaPercent) ''' 
            elif word == "bars_sold":
                sortby = "COUNT(*)"
                select_statement = '''SELECT Bars.Company, Countries.EnglishName, COUNT(*) '''
            elif "bottom" in word:
                split_word = word.split('=')
                number = int(split_word[1])
                sortby_query = " ORDER BY " + sortby + " LIMIT ?"
                bottom = True
            elif "top" in word:
                split_word = word.split('=')
                number = int(split_word[1])
                sortby_query = " ORDER BY " + sortby + " DESC LIMIT ?"    
            elif word == "ratings":
                continue
            elif word == "companies":
                continue
            else:
                print("Command not recognized: " + command)
                return

        if not bottom:
            sortby_query = " ORDER BY " + sortby + " DESC LIMIT ?"

        if country_query != "None":
            statement = select_statement + join_statement + country_query + groupby_query + sortby_query
            cur.execute(statement,(country,number))
        else:
            statement = select_statement + join_statement + groupby_query + sortby_query
            cur.execute(statement,(number,))    
        lst = cur.fetchall() 

    elif command_split[0] == "countries":
        select_statement = '''SELECT Countries.EnglishName, Countries.Region, AVG(Bars.Rating)'''
        join_statement = ''' FROM Bars JOIN Countries ON Bars.CompanyLocationId = Countries.Id WHERE Countries.EnglishName != "Unknown"'''
        groupby_query = ''' GROUP BY Countries.EnglishName HAVING COUNT(*) > 4'''
        sortby_query = ''' ORDER BY AVG(Bars.Rating) DESC LIMIT ?'''
        country = ""
        country_query = "None"
        sortby = "AVG(Bars.Rating)"
        number = 10
        bottom = False
        for word in command_split:
            if "region" in word:
                split_word = word.split('=')
                country = split_word[1]
                country_query = " AND Countries.Region = ?"
            elif word == "sources":
                join_statement = ''' FROM Bars JOIN Countries ON Bars.BroadBeanOriginId = Countries.Id WHERE Countries.EnglishName != "Unknown"'''
            elif word == "cocoa":
                select_statement = '''SELECT Countries.EnglishName, Countries.Region, AVG(Bars.CocoaPercent)'''
                sortby = "AVG(Bars.CocoaPercent)"
            elif word == "bars_sold":
                select_statement = '''SELECT Countries.EnglishName, Countries.Region, COUNT(SpecificBeanBarName) '''
                sortby = "COUNT(*)"
            elif "bottom" in word:
                split_word = word.split('=')
                number = int(split_word[1])
                sortby_query = " ORDER BY " + sortby + " LIMIT ?"
                bottom = True
            elif "top" in word:
                split_word = word.split('=')
                number = int(split_word[1])
                sortby_query = " ORDER BY " + sortby + " DESC LIMIT ?" 
            elif word == "ratings":
                continue
            elif word == "sellers":
                continue
            elif word == "countries":
                continue
            else:
                print("Command not recognized: " + command)
                return

        if not bottom:
            sortby_query = " ORDER BY " + sortby + " DESC LIMIT ?"

        if country_query != "None":
            statement = select_statement + join_statement + country_query + groupby_query + sortby_query
            cur.execute(statement,(country,number))
        else:
            statement = select_statement + join_statement + groupby_query + sortby_query
            cur.execute(statement,(number,))    
        lst = cur.fetchall() 

    elif command_split[0] == "regions":
        select_statement = '''SELECT Countries.Region, AVG(Bars.Rating)'''
        join_statement = ''' FROM Bars JOIN Countries ON Bars.CompanyLocationId = Countries.Id WHERE Countries.EnglishName != \"Unknown\"'''
        groupby_query = ''' GROUP BY Countries.Region HAVING COUNT(*) > 4'''
        sortby_query = ''' ORDER BY AVG(Bars.Rating) DESC LIMIT ?'''
        country = ""
        country_query = "None"
        sortby = "AVG(Bars.Rating)"
        number = 10
        bottom = False
        for word in command_split:
            if word == "sources":
                join_statement = ''' FROM Bars JOIN Countries ON Bars.BroadBeanOriginId = Countries.Id WHERE Countries.EnglishName != \"Unknown\"'''
            elif word == "cocoa":
                select_statement = '''SELECT Countries.Region, AVG(Bars.CocoaPercent)'''
                sortby = "AVG(Bars.CocoaPercent)"
            elif word == "bars_sold":
                select_statement = '''SELECT Countries.Region, COUNT(*) '''
                sortby = "COUNT(*)"
            elif "bottom" in word:
                split_word = word.split('=')
                number = int(split_word[1])
                sortby_query = " ORDER BY " + sortby + " LIMIT ?"
                bottom = True
            elif "top" in word:
                split_word = word.split('=')
                number = int(split_word[1])
                sortby_query = " ORDER BY " + sortby + " DESC LIMIT ?" 
            elif word == "ratings":
                continue
            elif word == "sellers":
                continue
            elif word == "regions":
                continue
            else:
                print("Command not recognized: " + command)
                return

        if not bottom:
            sortby_query = " ORDER BY " + sortby + " DESC LIMIT ?"

        statement = select_statement + join_statement + groupby_query + sortby_query
        cur.execute(statement,(number,))    
        
        lst = cur.fetchall() 
    
    else:
        print("Command not recognized: " + command)
        return
    
    conn.close()

    return lst


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    end = False
    while response != 'exit':
        response = input('Enter a command: ')
        if response != 'exit':
            result = process_command(response)
            if type(result) == list:
                tup_len = len(result[0])
                for tup in result:
                    x = 1
                    for word in tup:  
                        if x == tup_len:
                            end = True
                        else:
                            end = False
                        if type(word) == float:
                            word = round(word, 1)
                            if word <= 1 and x != 4:
                                word = word * 100
                                word = str(word)
                                word = word.split('.')
                                word = word[0] + "% "
                                if(end):
                                    print(word)
                                else:
                                    print(word, end = " ")
                            else:
                                word = str(word) + " "
                                if(end):
                                    print(word)
                                else:
                                    print(word, end = " ")
                        elif type(word) == str:
                            #From https://stackoverflow.com/questions/2872512/python-truncate-a-long-string
                            word = (word[:12] + '...') if len(word) > 12 else word
                            if(end):
                                print('{0: <15}'.format(word))
                            else:    
                                print('{0: <15}'.format(word), end = " ")
                        elif(end):
                            print(word)
                        else:
                            print(word, end = " ")
                        x += 1
        if response == 'help':
            print(help_text)
            continue
    return

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
