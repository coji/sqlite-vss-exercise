import sqlite3
import sqlite_vss
import json
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("pkshatech/simcse-ja-bert-base-clcmlp", device="cpu")


with open("sangokushi_structured.json") as f:
    sections = json.load(f)


db = sqlite3.connect("sangokushi.db", timeout=10)
db.enable_load_extension(True)
sqlite_vss.load(db)
vss_version = db.execute("select vss_version()").fetchone()[0]
print("SQLite VSS Version: %s" % vss_version)

db.execute(
    """
CREATE TABLE IF NOT EXISTS sections (
    id INTEGER PRIMARY KEY,
    volume_title TEXT,
    chapter_number TEXT,
    chapter_title TEXT,
    section_number TEXT,
    content TEXT
);
"""
)

db.execute(
    """
CREATE VIRTUAL TABLE IF NOT EXISTS vss_sections USING vss0(
    vector(768)
);
"""
)

for i, section in enumerate(sections):
    print(
        i + 1, section["volumeTitle"], section["chapterTitle"], section["sectionNumber"]
    )

    db.execute(
        """
INSERT INTO sections (
  id,
  volume_title,
  chapter_number,
  chapter_title,
  section_number,
  content)
VALUES (?, ?, ?, ?, ?, ?)
""",
        (
            i + 1,
            section["volumeTitle"],
            section["chapterNumber"],
            section["chapterTitle"],
            section["sectionNumber"],
            section["content"],
        ),
    )
    last_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    vector = model.encode(section["content"])
    db.execute(
        """
        INSERT INTO vss_sections (rowid, vector)
        VALUES (?, ?)
""",
        (last_id, vector),
    )
    db.commit()
