
CREATE TABLE Sequences (
   Key STRING(128) NOT NULL,
   Id INT64 NOT NULL,
) PRIMARY KEY (Key);

INSERT INTO Sequences (Key, Id) VALUES ("simple-sequencer", 0);
INSERT INTO Sequences (Key, Id) VALUES ("batch-sequencer", 0);
INSERT INTO Sequences (Key, Id) VALUES ("shard-sequencer_1", 0);
INSERT INTO Sequences (Key, Id) VALUES ("shard-sequencer_2", 0);
INSERT INTO Sequences (Key, Id) VALUES ("shard-sequencer_3", 0);