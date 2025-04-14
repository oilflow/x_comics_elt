import random


first_names = ["James", "John", "Robert", "Jessica", "Sarah", "David", "Sophia", "Kevin", "Kristin", "Nicholas",
               "Joost", "Willem", "Jorden", "Mike", "Angelica", "Abhishek", "Sharon"]
last_names = ["Smith", "Jones", "Brown", "Johnson",  "Miller", "Bond", "Bean", "White", "Nakamura", "Kumar",
              "Jefferson", "Michelin", "Mathew", "Fisher", "McDonald"]


def generate_random_author():
    """
    Generate a random author with a random name and email.
    """
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)

    author_name = f"{first_name} {last_name}"

    email = f"{first_name.lower()}.{last_name.lower()}@author.com"

    return author_name, email

