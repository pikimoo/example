#!/usr/bin/python

from collections import defaultdict
import psycopg2
from lib.row import Row
from lib.score import Score

# Helper function to read file
def _linesFromFile():
    with open("data/reviews.csv") as f:
        review_file = f.read()
    lines = review_file.split("\n")

    # Remove header row
    lines.pop(0)

    return lines

##################################################

# Connect to database

conn = psycopg2.connect(
    "host=%s dbname=%s user=%s password=%s" %
    ("localhost", "roverdb", "rover", "woof")
)
cursor = conn.cursor()

all_sitters = {}
all_owners = {}
sitter_ratings = defaultdict(list)

# Insert reviews
INSERT_REVIEW = """
    insert into reviews (
        rating, start_date, end_date, review, dogs, sitter, owner
    )
    values (%s, %s, %s, %s, %s, %s, %s)
"""

for line in _linesFromFile():
    if len(line) == 0:
        continue

    # Create row helper
    row = Row(line)

    # Get the sitter ID from the sitter image
    # Store it so we can put it in a separate table later
    sitter_image = row.val_by_name("sitter_image")
    _, sitter_id = sitter_image.split("user=")
    all_sitters[sitter_id] = (row.val_by_name("sitter"), sitter_image)

    # Get the sitter ratings, and store it for later
    sitter_ratings[sitter_id].append(row.val_by_name("rating"))

    # Get the owner ID from the owner image
    owner_image = row.val_by_name("owner_image")
    _, owner_id = owner_image.split("user=")
    all_owners[owner_id] = (row.val_by_name("owner"), owner_image)

    cursor.execute(INSERT_REVIEW, [
        row.val_by_name("rating"),
        row.val_by_name("start_date"),
        row.val_by_name("end_date"),
        row.val_by_name("text"),
        row.val_by_name("dogs"),
        sitter_id,
        owner_id
    ])

# Insert into sitters table
INSERT_SITTER = """
    insert into sitters(id, name, image_link) values (%s, %s, %s)
"""

for sitter_id in all_sitters:
    name, image = all_sitters[sitter_id]
    cursor.execute(INSERT_SITTER, [ sitter_id, name, image ])

# Insert into owners table

INSERT_OWNER = """
    insert into owners(id, name, image_link) values (%s, %s, %s)
"""
for owner_id in all_owners:
    name, image = all_owners[owner_id]
    cursor.execute(INSERT_OWNER, [ owner_id, name, image ])

# Calculate scores
INSERT_SCORE = "insert into sitter_scores(id, score) values (%s, %s)"

for sitter_id in sitter_ratings:
    name, _ = all_sitters[sitter_id]
    ratings = sitter_ratings[sitter_id]
    score = Score.calculate_score(name, ratings)
    cursor.execute(INSERT_SCORE, [ sitter_id, score ])

conn.commit()
