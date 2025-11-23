import sys
import random

random_names = ["John", "Mary", "Peter", "Paul", "George", "Ringo", "Bob", "Jane", "Bill", "Steve", "Larry", "Mike", "Tom", "Jim", "Joe", "Jack", "Kate", "Anne", "Diane", "Sarah", "Pamela", "Carol", "Haley", "Susan", "Janet", "Rose", "Elizabeth", "Jennifer", "Maria", "Deborah", "Theresa", "Rachel", "Barbara", "Virginia", "Christine", "Angela", "Martha", "Denise", "Lisa", "Tina", "Cindy", "Margaret", "Yolanda", "Catherine", "Julie", "Heather", "Monica", "Emma", "Doris", "Anna", "Shirley", "Ruth", "Betty", "Helen", "Frances", "Martha", "Evelyn", "Jean", "Marie", "Irene", "Florence", "Alice", "Louise", "Rose", "Clara", "Bertha", "Grace", "Bertha", "Bessie", "Hazel", "Sara", "Viola", "Ada", "Sylvia", "Lillian", "Myrtle", "Laura", "Carrie", "Lucille", "Mabel", "Edith", "Pearl", "Ruby", "Esther", "Minnie", "Lucille", "Elli"]

random_last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "Hernandez", "King", "Wright", "Lopez", "Hill", "Scott", "Green", "Adams", "Baker", "Gonzalez", "Nelson", "Carter", "Mitchell", "Perez", "Roberts", "Turner", "Phillips", "Campbell", "Parker", "Evans", "Edwards", "Collins", "Stewart", "Sanchez", "Morris", "Rogers", "Reed", "Cook", "Morgan", "Bell", "Murphy", "Bailey", "Rivera", "Cooper", "Richardson", "Cox", "Howard", "Ward", "Torres", "Peterson", "Gray", "Ramirez", "James", "Watson", "Brooks", "Kelly", "Sanders", "Price", "Bennett", "Wood", "Barnes", "Ross", "Henderson", "Coleman", "Jenkins", "Perry", "Powell", "Long", "Patterson", "Hughes", "Flores", "Washington", "Butler", "Simmons", "Foster", "Gonzales", "Bryant", "Alexander", "Russell", "Griffin", "Diaz", "Hayes"]

random_locations = ["Boston", "New York", "Chicago", "Philadelphia", "San Francisco", "Seattle", "Los Angeles", "Denver", "Atlanta", "Miami", "Houston", "Dallas", "Washington", "Detroit", "Phoenix", "Las Vegas", "Baltimore", "Cleveland", "Tampa", "Charlotte", "Orlando", "Milwaukee", "Portland", "Albuquerque", "San Diego", "Jacksonville", "Pittsburgh", "Fresno", "Sacramento", "Long Beach", "Reno", "Memphis", "Kansas City", "St. Louis", "Amarillo", "Fort Worth", "Oklahoma City", "Tucson", "Sherman", "El Paso", "Colorado Springs", "Arlington", "North Las Vegas", "Greensboro", "Winston-Salem", "Birmingham", "Raleigh", "Augusta", "Columbus", "Huntington", "Salt Lake City", "Montgomery", "Louisville", "Nashville", "Birmingham", "Providece"]

# First output the create table statement
sys.stdout.write("create stores.dat\nstore_id\n1\nfirst_name\n3\n15\nlast_name\n3\n15\nlocation\n3\n32\nprofit\n2\nfinish\nopen stores.dat\n")

# Then output the insert statements
for i in range(0, 5000):
    sys.stdout.write(f"stores insert {i},{random.choice(random_names)},{random.choice(random_last_names)},{random.choice(random_locations)},{random.uniform(-10_000, 500_000)}\n")

sys.stdout.write("exit\n")
