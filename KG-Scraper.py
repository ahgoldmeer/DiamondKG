import requests
from bs4 import BeautifulSoup
import csv
from neo4j import GraphDatabase
import re
import json
import os

# --- Global Patterns ---
with open('patterns.json', 'r') as f:
    patterns = json.load(f)

b_t_pattern = patterns['b_t']
b_t_extra_pattern = patterns['b_t_extra']
year_pattern = patterns['year']
year_extra1_pattern = patterns['year_extra_1']
year_extra2_pattern = patterns['year_extra_2']
position_pattern = patterns['positions']
majors = patterns['majors']
height_pattern = r"\d' ?\d{1,2}''" # height pattern to handle both formats: 6'3'' and 6' 3''
weight_pattern = r"\d{3}" # weight pattern for 3 digit weights
# --- End Global Patterns ---

def write_to_neo4j(data, school, year):
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
                """
                MATCH (s:School {name: $school}), (c:Conference {name: $conference})               
                MERGE (s)-[:MEMBER_OF]->(c)
                """,
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
            major = None

            if values[0].isnumeric(): # Add players, which always start with their number
                for value in range(2, len(values)):
                    if (values[value] in b_t_pattern): # bat/throw values
                        bat_throw = values[value].split('/')
                    elif values[value] in b_t_extra_pattern: # standardize format
                        index = b_t_extra_pattern.index(values[value])
                        bat_throw = b_t_pattern[index].split('/')

                    elif (values[value] in year_pattern): # Year
                        player_year = values[value]
                    elif (values[value] in year_extra1_pattern): # standardize format
                        index = year_extra1_pattern.index(values[value])
                        player_year = year_pattern[index]
                    elif (values[value] in year_extra2_pattern): # standardize format
                        index = year_extra2_pattern.index(values[value])
                        player_year = year_pattern[index]

                    elif re.match(height_pattern, values[value]): # height values
                        height = values[value]

                    elif re.match(weight_pattern, values[value]): # weight values
                        weight = values[value]

                    elif values[value] in position_pattern: # position values
                        if '/' in values[value]:
                            position = values[value].split('/')  # Split into a list if '/' is present
                        else:
                            position = [values[value]]  # Wrap in a single-element list if no '/'
                    elif values[value] in majors: # major values
                        major = values[value]

                    elif '/' in values[value]: # hometown + previous school values
                        hometown, high_school = map(str.strip, values[value].split('/', 1))
                    else:
                        if re.match(r"^[A-Za-z ]+, [A-Za-z]{2,15}\.?$", values[value].strip()):  # Matches [City, State] pattern
                            hometown = values[value]
                        if re.search(r"High School|College|University|Academy|State", values[value], re.IGNORECASE):  # Matches school-related keywords
                            high_school = values[value]

                # )
                session.run(
                    """
                    MERGE (p:Player {name: $name})
                    ON CREATE SET p.hometown = $hometown, p.high_school = $high_school
                    """,
                    name=values[1] if len(values) > 1 else None,
                    hometown=hometown,
                    high_school=high_school
                )
                session.run(
                    """
                    MATCH (p:Player {name: $name})
                    MATCH (t:Team {name: $team})
                    CREATE (p)-[:PLAYED_FOR {
                        year: $year,
                        player_year: $player_year,
                        number: $num,
                        position: $position,
                        height: $height,
                        weight: $weight,
                        bat_throw: $bat_throw,
                        major: $major
                    }]->(t)
                    """, 
                    name=values[1] if len(values) > 1 else None,
                    team=team,
                    year=year,
                    player_year=player_year,
                    num=values[0] if len(values) > 0 else None,
                    position=position,
                    height=height,
                    weight=weight,
                    bat_throw=bat_throw,
                    major=major
                )
                session.run(
                    """
                    MATCH (p:Player {name: $name})
                    MATCH (s:School {name: $school})
                    MERGE (p)-[:ATTENDS]->(s)
                    """, 
                    name=values[1] if len(values) > 1 else None, 
                    school=school
                )
            elif values[0] != 'Skip Ad': # Add coaches, which never start with a number
                name=values[1] if len(values) > 0 else None,
                title=values[2] if len(values) > 1 else None,
                email=values[3] if len(values) > 2 else None,
                phone_number=values[4] if len(values) > 3 else None
                session.run(
                    """
                    MERGE (c:Coach {name: $name})
                    ON CREATE SET c.email = $email, c.phone_number = $phone, c.title = $title
                    """, 
                    name=name, 
                    email=email,
                    phone=phone_number,
                    title = title
                )
                session.run(
                    """
                    MATCH (c:Coach {name: $name})
                    MATCH (t:Team {name: $team})
                    MERGE (c)-[:COACHES_FOR]->(t)
                    """, 
                    name=name, 
                    team=team)
                session.run(
                    """
                    MATCH (c:Coach {name: $name})
                    MATCH (s:School {name: $school})
                    MERGE (c)-[:COACHES_AT]->(s)
                    """, 
                    name=name, 
                    school=school
                    )

    driver.close()

def scrape():
    with open('school-info.json', 'r') as f:
        data = json.load(f)
    urls = [entry['url'] for entry in data]
    schools = [entry['name'] for entry in data]
    for u, school in zip(urls, schools):
        response = requests.get(u)
        soup = BeautifulSoup(response.text, 'html.parser')
        data = soup.select('tr[class*="s-table-body__row"]')
        print("Scraping school:", school)

        # Determine the year based on the URL
        year = 2025 if "2025" in u else 2026

        write_to_neo4j(data, school, year)

        # Check if the URL ends with '/2025' and adjust the filename accordingly
        csv_suffix = "_2025" if u.endswith("/2025") else ""
        filepath = os.path.join("CSVs", f"{school}{csv_suffix}.csv")

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