CREATE TABLE IF NOT EXISTS characters (
  race TEXT,
  name TEXT,
  proficiences TEXT,
  language TEXT,
  spells TEXT,
  class TEXT,
  levels TEXT,
  attributes TEXT
);
INSERT INTO characters VALUES ('gnome', 'Gail', $$['skill-insight', 'skill-medicine']$$, 'common', $$['regenerate']$$, 'druid', '2', $$[17, 8, 10, 2, 4]$$);
INSERT INTO characters VALUES ('half-orc', 'Robert', $$['skill-athletics', 'skill-arcana', 'skill-persuasion']$$, 'dwarvish', $$['awaken', 'hypnotic-pattern']$$, 'bard', '2', $$[12, 14, 11, 12, 13]$$);
INSERT INTO characters VALUES ('human', 'Lindsay', $$['skill-athletics', 'skill-religion']$$, 'primordial', $$['bless', 'heroism']$$, 'paladin', '1', $$[10, 10, 10, 4, 14]$$);
INSERT INTO characters VALUES ('tiefling', 'Tamara', $$['skill-athletics', 'skill-insight', 'skill-investigation']$$, 'giant', $$['detect-poison-and-disease', 'alarm']$$, 'ranger', '3', $$[18, 11, 6, 12, 13]$$);
INSERT INTO characters VALUES ('half-elf', 'Brittany', $$['skill-medicine', 'skill-investigation']$$, 'celestial', $$['arcane-sword']$$, 'wizard', '2', $$[18, 5, 13, 8, 10]$$);
