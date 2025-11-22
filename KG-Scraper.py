import requests
from bs4 import BeautifulSoup
import csv
from neo4j import GraphDatabase
import re
import json
import os

# --- Global Patterns ---
b_t = ['R/R', 'L/L', 'R/L', 'L/R', 'S/L', 'S/R', 'L/S', 'R/S']
b_t_extra = ['R-R', 'L-L', 'R-L', 'L-R', 'S-L', 'S-R', 'L-S', 'R-S']
year = ['Fr', 'So', 'Jr', 'Sr', 'Fr.', 'So.', 'Jr.', 'Sr.', 'FR', 'SO', 'JR', 'SR']
year_extra =['R-Fr.', 'R-So.', 'R-Jr.', 'R-Sr.', 'Gr.', 'GR.', 'RS FR', 'RS JR', 'RS SO', 'RS SR']
height_pattern = r"\d' ?\d{1,2}''" # height pattern to handle both formats: 6'3'' and 6' 3''
position_list = ['INF', 'RHP', 'LHP', 'OF', 'C', 'P', '1B', 'UTL', 'UT', 'INF/RHP', 'INF/LHP', 'INF/OF', 'INF/C', 'INF/P', 'INF/1B', 'INF/UTL', 'INF/UT', 'RHP/INF', 'RHP/LHP', 'RHP/OF', 'RHP/C', 'RHP/P', 'RHP/1B', 'RHP/UTL', 'RHP/UT', 'LHP/INF', 'LHP/RHP', 'LHP/OF', 'LHP/C', 'LHP/P', 'LHP/1B', 'LHP/UTL', 'LHP/UT', 'OF/INF', 'OF/RHP', 'OF/LHP', 'OF/C', 'OF/P', 'OF/1B', 'OF/UTL', 'OF/UT', 'C/INF', 'C/RHP', 'C/LHP', 'C/OF', 'C/P', 'C/1B', 'C/UTL', 'C/UT', 'P/INF', 'P/RHP', 'P/LHP', 'P/OF', 'P/C', 'P/1B', 'P/UTL', 'P/UT', '1B/INF', '1B/RHP', '1B/LHP', '1B/OF', '1B/C', '1B/P', '1B/UTL', '1B/UT', 'UTL/INF', 'UTL/RHP', 'UTL/LHP', 'UTL/OF', 'UTL/C', 'UTL/P', 'UTL/1B', 'UTL/UT', 'UT/INF', 'UT/RHP', 'UT/LHP', 'UT/OF', 'UT/C', 'UT/P', 'UT/1B', 'UT/UTL']
weight_pattern = r"\d{3}" # weight pattern for 3 digit weights
# --- End Global Patterns ---

def write_to_neo4j(data, school):
    uri = "neo4j://127.0.0.1:7687" 
    username = "neo4j" 
    password = "password"

    driver = GraphDatabase.driver(uri, auth=(username, password))

    with driver.session() as session:
        conference, team, state, field = get_details(school)
        team = school + " " + team
        session.run(
            "MERGE (s:School {name: $school, state: $state})",
            school=school,
            state=state
        )
        session.run(
            "MERGE (t:Team {name: $team, field: $field})",
            team=team,
            field=field
        )
        if conference:
            session.run(
                "MERGE (c:Conference {name: $conference})",
                conference=conference
            )
            session.run(
                "MATCH (s:School {name: $school}), (c:Conference {name: $conference})\n                 MERGE (s)-[:MEMBER_OF]->(c)",
                school=school,
                conference=conference
            )
        for row in data:
            columns = row.find_all('td') 
            values = [col.text.strip() for col in columns]

            bat_throw = None
            player_year = None
            height = None
            position = None
            hometown = None
            high_school = None
            weight = None

            if values[0].isnumeric(): # Add players, which always start with their number
                for value in range(2, len(values)):
                    if values[value] in b_t: # bat/throw values
                        bat_throw = values[value]
                    elif values[value] in b_t_extra: # replace alternate bat/throw values with single format
                        index = b_t_extra.index(values[value])
                        bat_throw = b_t[index]
                    elif values[value] in year or values[value] in year_extra: # year values --> Need to standardize format
                        player_year = values[value]
                    elif re.match(height_pattern, values[value]): # height values
                        height = values[value]
                    elif re.match(weight_pattern, values[value]): # weight values
                        weight = values[value]
                    elif values[value] in position_list: # position values --> Should probably standardize these too
                        position = values[value]
                    elif '/' in values[value]:
                        hometown, high_school = map(str.strip, values[value].split('/', 1))
                    else:
                        if re.match(r"^[A-Za-z ]+, [A-Za-z]{2,15}\.?$", values[value].strip()):  # Matches [City, State] pattern
                            hometown = values[value]
                        if re.search(r"High School|College|University|Academy|State", values[value], re.IGNORECASE):  # Matches school-related keywords
                            high_school = values[value]

                # # Extract hometown and high school
                # if '/' in values[-1]:
                #     hometown, high_school = map(str.strip, values[-1].split('/', 1))
                # else:
                #     hometown = values[-1]

                session.run(
                    """
                    MERGE (p:Player {number: $num, name: $name, team: $team})
                    SET p.bat_throw = $bat_throw, p.year = $year, p.height = $height, p.weight = $weight, p.position = $position,
                        p.hometown = $hometown, p.high_school = $high_school
                    """,
                    team=team,
                    num=values[0] if len(values) > 0 else None,
                    name=values[1] if len(values) > 1 else None,
                    bat_throw=bat_throw,
                    year=player_year,
                    height=height,
                    weight=weight,
                    position=position,
                    hometown=hometown,
                    high_school=high_school
                )
                session.run( # Create relationship between player and team
                    """
                    MATCH (p:Player {number: $num, name: $name, team: $team})
                    MATCH (t:Team {name: $team})
                    MERGE (p)-[:PLAYS_FOR]->(t)
                    """,
                    team=team,
                    num=values[0] if len(values) > 0 else None,
                    name=values[1] if len(values) > 1 else None
                )
                session.run( # Create relationship between player and school
                    """
                    MATCH (p:Player {number: $num, name: $name, team: $team})
                    MATCH (s:School {name: $school})
                    MERGE (p)-[:ATTENDS]->(s)
                    """,
                    team=team,
                    num=values[0] if len(values) > 0 else None,
                    name=values[1] if len(values) > 1 else None,
                    school=school
                )
            elif values[0] != 'Skip Ad': # Add coaches, which never start with a number
                session.run(
                    "MERGE (:Coach {team: $team, name: $name, title: $title, email: $email, phone_number: $phone_number})",
                    team=team,
                    name=values[1] if len(values) > 0 else None,
                    title=values[2] if len(values) > 1 else None,
                    email=values[3] if len(values) > 2 else None,
                    phone_number=values[4] if len(values) > 3 else None
                )
                session.run( # Create relationship between coach and school
                    """
                    MATCH (c:Coach {team: $team, name: $name})
                    MATCH (s:School {name: $school})
                    MERGE (c)-[:COACHES_AT]->(s)
                    """,
                    team=team,
                    name=values[1] if len(values) > 0 else None,  # Correctly pass the name parameter
                    school=school
                )
                session.run(
                    """
                    MATCH (c:Coach {team: $team, name: $name})
                    MATCH (t:Team {name: $team})
                    MERGE (c)-[:COACHES_FOR]->(t)
                    """,
                    team=team,
                    name=values[1] if len(values) > 0 else None
                )
    driver.close()

def scrape():
    with open('info.json', 'r') as f:
        data = json.load(f)
    urls = [entry['url'] for entry in data]
    schools = [entry['name'] for entry in data]
    for u, school in zip(urls, schools):
        response = requests.get(u)
        soup = BeautifulSoup(response.text, 'html.parser')
        data = soup.select('tr[class*="s-table-body__row"]')
        print("Scraping school:", school)
        write_to_neo4j(data, school)
        filepath = os.path.join("CSVs", f"{school}.csv")
        with open(filepath, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Column1', 'Column2', 'Column3', '...'])
            for row in data:
                columns = row.find_all('td')
                writer.writerow([col.text.strip() for col in columns])

def get_details(school_name):
    url = "https://en.wikipedia.org/wiki/List_of_NCAA_Division_I_baseball_programs"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'wikitable'})
    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if not cells:
            continue
        school_text = cells[0].get_text(strip=True)
        if school_name in school_text:
            conference = cells[4].get_text(strip=True)
            team = cells[1].get_text(strip=True).replace(" ", "_")
            state = cells[2].get_text(strip=True)
            field = cells[3].get_text(strip=True)
            return conference, team, state, field


if __name__ == '__main__':
    scrape()