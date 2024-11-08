import pytest
import os
import shutil
from railbe.raildb import RailDB

def test_raildb():
    assert not os.path.exists("nonexistent")
    db = RailDB(dbname = "test.db", dir = "nonexistent")
    assert os.path.exists("nonexistent")
    assert db.tables == []
    db.close()
    shutil.rmtree("nonexistent")
